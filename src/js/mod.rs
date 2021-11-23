use crossbeam::queue::SegQueue;
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, ensure};
use deno_core::{RuntimeOptions, op_sync, v8::CreateParams};

use crate::runtime::{Limits, QueryRuntime, Value};

/// A [`QueryRuntime`] for JavaScript using Deno/V8.
/// `glue.js` is run before every query.
/// A new runtime/engine is created for every request.
pub struct JSQueryRuntime {
    limits: Limits
}

impl JSQueryRuntime {
    /// Create a new [`JSQueryRuntime`].
    /// This doesn't actually do much - everything is created in [`QueryRuntime::execute`],
    /// since the JavaScript runtime itself is not [`Send`].
    pub fn new() -> Self {
        Self {
            limits: Limits::default()
        }
    }
}

impl QueryRuntime for JSQueryRuntime {
    fn set_limits(&mut self, limits: &Limits) -> &Limits {
        /* 
         * TODO: V8 doesn't handle OOM conditions well.
         * Instead, it just kills the entire process.
         * For perfect sandboxing, we need to run this in a child process, which could get very messy.
         * We don't currently do that, so conditions that make V8 run out of memory kill the entire server.
         */
        self.limits.heap = limits.heap;
        self.limits.time = limits.time;

        &self.limits
    }

    fn execute(
        &self,
        script: String,
        database: Arc<dyn crate::database::Database>,
    ) -> Box<dyn Iterator<Item = anyhow::Result<crate::runtime::Value>>> {
        let results = Arc::new(SegQueue::new());

        {
            let results = results.clone();
            let max_heap = self.limits.heap;
            let max_time = self.limits.time;
            std::thread::spawn(move || {
                let mut runtime = {
                    let mut create_params = CreateParams::default();

                    if let Some(heap) = max_heap {
                        create_params = create_params.heap_limits(0, heap);
                    }
                    
                    deno_core::JsRuntime::new(RuntimeOptions {
                        create_params: Some(create_params),
                        ..Default::default()
                    })
                };
                
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
                    
                    if let Some(time) = max_time {
                        let handle = runtime.v8_isolate().thread_safe_handle();

                        let doom = tokio::time::Instant::now() + std::time::Duration::new(
                            time.trunc() as u64, 
                            ((time / 1_000_000_000.0) % (1_000_000_000.0)).trunc() as u32
                        );

                        rt.spawn(async move {
                            tokio::time::sleep_until(doom).await;

                            handle.terminate_execution();
                        });
                    }

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
