use anyhow::anyhow;
use async_trait::async_trait;
use deno_core::{op_async, JsRuntime, RuntimeOptions};
use hmac::{Hmac, Mac, NewMac};
use scopeguard::defer;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::Sha256;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::{spawn_local, JoinHandle};

use crate::ringbuffer::RingBuffer;

#[async_trait]
pub trait BatchedStreamProvider<I, T>
where
    I: Serialize + DeserializeOwned + Clone,
    T: Serialize,
{
    /* the type I might represent an index in a file, etc. */
    async fn begin(&self) -> anyhow::Result<I>;
    async fn next(&self, pos: &mut I) -> anyhow::Result<Vec<T>>;
    async fn more(&self, pos: &I) -> anyhow::Result<bool>;
}

type HmacSha256 = Hmac<Sha256>;

#[derive(Serialize, Deserialize, Clone)]
struct Secured {
    data: String,
    sig: String,
}

impl Secured {
    pub fn new(mut hmac: HmacSha256, data: String) -> Self {
        hmac.update(data.clone().into_bytes().as_slice());
        Self {
            data,
            sig: hex::encode(hmac.finalize().into_bytes()),
        }
    }

    pub fn ensure(&self, mut hmac: HmacSha256) -> bool {
        hex::decode(self.sig.clone())
            .ok()
            .and_then(|sig| {
                let slice = sig.as_slice();
                hmac.update(self.data.clone().into_bytes().as_slice());
                hmac.verify(slice).ok()
            })
            .is_some()
    }
}

type ResultsBuffer = RingBuffer<serde_json::Value, 1024>;

pub struct DatabaseWrapper {
    runtime: JsRuntime,
    pub results: Arc<ResultsBuffer>,
}

impl DatabaseWrapper {
    pub fn new<I, T, B>(db: Arc<B>) -> anyhow::Result<Self>
    where
        I: Serialize + DeserializeOwned + Clone + 'static,
        T: Serialize + 'static,
        B: BatchedStreamProvider<I, T> + 'static,
    {
        let mac = HmacSha256::new_from_slice(rand::random::<[u8; 32]>().as_slice()).unwrap();
        let mut runtime = JsRuntime::new(RuntimeOptions::default());
        let results = Arc::new(ResultsBuffer::new());

        {
            let db = db.clone();
            let mac = mac.clone();
            runtime.register_op(
                "database.begin",
                op_async(move |_state, _: (), _: ()| {
                    let db = db.clone();
                    let mac = mac.clone();
                    async move {
                        let data = {
                            let tmp = db.begin().await?;
                            serde_json::to_string(&tmp)?
                        };

                        Ok(Secured::new(mac, data))
                    }
                }),
            );
        }

        {
            let db = db.clone();
            let mac = mac.clone();
            runtime.register_op(
                "database.next",
                op_async(move |_state, s: Secured, _: ()| {
                    let db = db.clone();
                    let mac = mac.clone();
                    async move {
                        if !s.ensure(mac.clone()) {
                            return Err(anyhow!("signature does not match"));
                        }

                        let mut i: I = serde_json::from_str(s.data.as_str())?;

                        let stuff = db.next(&mut i).await?;

                        Ok((Secured::new(mac.clone(), serde_json::to_string(&i)?), stuff))
                    }
                }),
            );
        }

        {
            //let db = db.clone();
            //let mac = mac.clone();
            runtime.register_op(
                "database.more",
                op_async(move |_state, s: Secured, _: ()| {
                    let db = db.clone();
                    let mac = mac.clone();
                    async move {
                        if !s.ensure(mac.clone()) {
                            return Err(anyhow!("signature does not match"));
                        }

                        let i: I = serde_json::from_str(s.data.as_str())?;

                        db.more(&i).await
                    }
                }),
            );
        }

        {
            let results = results.clone();
            runtime.register_op(
                "database.send",
                op_async(move |_state, batch: Vec<serde_json::Value>, _: ()| {
                    let results = results.clone();
                    async move {
                        results.produce_many(&mut batch.into_iter()).await;

                        Ok(())
                    }
                }),
            );
        }

        runtime.sync_ops_cache();
        runtime.execute_script("[native code]", include_str!("glue.js"))?;

        Ok(Self { runtime, results })
    }

    pub async fn spawn<I, T, B>(
        db: Arc<B>,
        script: oneshot::Receiver<String>,
    ) -> anyhow::Result<(JoinHandle<anyhow::Result<()>>, Arc<ResultsBuffer>)>
    where
        I: Serialize + DeserializeOwned + Clone + 'static,
        T: Serialize + 'static,
        B: BatchedStreamProvider<I, T> + 'static,
    {
        let (send, recv) = oneshot::channel();
        Ok((
            spawn_local(async move {
                let wrapper = Self::new(db)?;

                let mut runtime = wrapper.runtime;
                let results = wrapper.results;

                defer! {
                    results.close();
                }

                send.send(results.clone())
                    .map_err(|_| anyhow!("could not send value"))?;

                let query = script.await?;
                runtime.execute_script("[query]", query.as_str())?;
                runtime.run_event_loop(false).await?;

                Ok(())
            }),
            recv.await?,
        ))
    }
}
