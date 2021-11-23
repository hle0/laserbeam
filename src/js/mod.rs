use crossbeam::queue::SegQueue;
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, ensure};
use deno_core::op_sync;

use crate::runtime::{QueryRuntime, Value};

pub struct JSQueryRuntime {}

impl JSQueryRuntime {
    pub fn new() -> Self {
        Self {}
    }
}

impl QueryRuntime for JSQueryRuntime {
    fn execute(
        &self,
        script: String,
        database: Arc<dyn crate::database::Database>,
    ) -> Box<dyn Iterator<Item = anyhow::Result<crate::runtime::Value>>> {
        let results = Arc::new(SegQueue::new());

        {
            let results = results.clone();
            std::thread::spawn(move || {
                let mut runtime = deno_core::JsRuntime::new(Default::default());
                let iterators = Arc::new(Mutex::new(HashMap::new()));

                {
                    let iterators = iterators.clone();
                    let database = database.clone();
                    runtime.register_op(
                        "database.begin",
                        op_sync(move |_, which: String, _: ()| {
                            let idx = rand::random::<u32>();
                            ensure!(
                                iterators
                                    .lock()
                                    .insert(idx, database.get_table(which.as_str())?)
                                    .is_none(),
                                "index for iterator already exists somehow??"
                            );
                            Ok(idx)
                        }),
                    );
                }

                {
                    let iterators = iterators.clone();
                    runtime.register_op(
                        "database.next",
                        op_sync(move |_, idx: u32, _: ()| {
                            let mut lock = iterators.lock();
                            let iterator = lock
                                .get_mut(&idx)
                                .ok_or_else(|| anyhow!("no such iterator"))?;
                            Ok(iterator.next())
                        }),
                    );
                }

                {
                    let results = results.clone();
                    runtime.register_op(
                        "database.send",
                        op_sync(move |_, item: Value, _: ()| {
                            results.push(Some(Ok(item)));
                            Ok(())
                        }),
                    );
                }

                runtime.sync_ops_cache();
                let result: anyhow::Result<()> = try {
                    runtime.execute_script("[native code]", include_str!("glue.js"))?;
                    runtime.execute_script("[query]", script.as_str())?;

                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();

                    rt.block_on(async move { runtime.run_event_loop(false).await })?;
                };

                if let Err(e) = result {
                    results.push(Some(Err(e)));
                }

                results.push(None);
            });
        }

        Box::new(std::iter::from_fn(move || loop {
            if let Some(thing) = results.pop() {
                break thing;
            }

            std::thread::yield_now();
        }))
    }
}
