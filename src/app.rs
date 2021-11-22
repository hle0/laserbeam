use std::sync::Arc;

use crate::database::JSONLinesStreamingBackend;
use crate::js::DatabaseWrapper;
use anyhow::anyhow;
use rocket::{post, State};
use tokio::{sync::oneshot, task::LocalSet};

pub type DatabaseBackend = JSONLinesStreamingBackend;

#[post("/query", data = "<script>")]
pub async fn query(script: String, db: &State<Arc<DatabaseBackend>>) -> String {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let (rt_send, rt_recv) = oneshot::channel();

    let db = db.inner().clone();
    std::thread::spawn(move || {
        let local = LocalSet::new();
        local.spawn_local(async move {
            let result: anyhow::Result<String> = try {
                let (send, recv) = oneshot::channel();
                let (join, results) = DatabaseWrapper::spawn(db, recv).await?;
                send.send(script).map_err(|_| anyhow!("could not send"))?;

                let mut vec = Vec::new();
                while let Some(thing) = results.consume_one().await {
                    vec.push(thing);
                }

                join.await??;

                serde_json::to_string(&vec)?
            };

            rt_send
                .send(match result {
                    Ok(x) => x,
                    Err(x) => x.to_string(),
                })
                .unwrap();
        });

        rt.block_on(local);
    });

    match rt_recv.await {
        Ok(x) => x,
        Err(x) => x.to_string(),
    }
}
