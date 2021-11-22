use deno_core::{JsRuntime, RuntimeOptions, op_async};
use hmac::{Hmac, Mac, NewMac};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use async_trait::async_trait;
use sha2::Sha256;
use std::{marker::PhantomData, sync::Arc};
use anyhow::anyhow;
use tokio::task::{JoinHandle, spawn_local};
use tokio::sync::oneshot;

use crate::ringbuffer::RingBuffer;

#[async_trait]
pub trait BatchedStreamProvider<I, T> where I: Serialize + DeserializeOwned + Clone, T: Serialize {
    /* the type I might represent an index in a file, etc. */
    async fn begin(&self) -> anyhow::Result<I>;
    async fn next(&self, pos: &mut I) -> anyhow::Result<Vec<T>>;
    async fn more(&self, pos: &I) -> anyhow::Result<bool>;
}

type HmacSha256 = Hmac<Sha256>;

#[derive(Serialize, Deserialize, Clone)]
struct Secured {
    data: String,
    sig: String
}

impl Secured {
    pub fn new(mut hmac: HmacSha256, data: String) -> Self {
        hmac.update(data.clone().into_bytes().as_slice());
        Self {
            data,
            sig: hex::encode(hmac.finalize().into_bytes())
        }
    }

    pub fn ensure(&self, mut hmac: HmacSha256) -> bool {
        hex::decode(self.sig.clone()).ok().and_then(|sig| {
            let slice = sig.as_slice();
            hmac.update(self.data.clone().into_bytes().as_slice());
            hmac.verify(slice).ok()
        }).is_some()
    }
}

type ResultsBuffer = RingBuffer<String, 1024>;

pub struct DatabaseWrapper<I, T, B> where I: Serialize + DeserializeOwned + Clone, T: Serialize, B: BatchedStreamProvider<I, T> {
    db: Arc<B>,
    /* to ensure that we aren't feeding bogus/malicious data to the database, we need this */
    mac: HmacSha256,
    runtime: JsRuntime,
    pub results: Arc<ResultsBuffer>,
    _i: PhantomData<I>,
    _t: PhantomData<T>
}

impl<I, T, B> DatabaseWrapper<I, T, B> where I: Serialize + DeserializeOwned + Clone + 'static, T: Serialize + 'static, B: BatchedStreamProvider<I, T> + 'static {
    pub fn new(db: Arc<B>) -> anyhow::Result<Self> {
        let mac = HmacSha256::new_from_slice(rand::random::<[u8; 32]>().as_slice()).unwrap();
        let mut runtime = JsRuntime::new(RuntimeOptions::default());
        let results = Arc::new(ResultsBuffer::new());

        {
            let db = db.clone();
            let mac = mac.clone();
            runtime.register_op("database.begin", op_async(move |_state, _: (), _: ()| {
                let db = db.clone();
                let mac = mac.clone();
                async move {
                    let data = {
                        let tmp = db.begin().await?;
                        serde_json::to_string(&tmp)?
                    };
            
                    Ok(Secured::new(mac, data))
                }
            }));
        }

        {
            let db = db.clone();
            let mac = mac.clone();
            runtime.register_op("database.next", op_async(move |_state, s: Secured, _: ()| {
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
            }));
        }

        {
            let db = db.clone();
            let mac = mac.clone();
            runtime.register_op("database.more", op_async(move |_state, s: Secured, _: ()| {
                let db = db.clone();
                let mac = mac.clone();
                async move {
                    if !s.ensure(mac.clone()) {
                        return Err(anyhow!("signature does not match"));
                    }
            
                    let i: I = serde_json::from_str(s.data.as_str())?;
            
                    db.more(&i).await
                }
            }));
        }

        {
            let results = results.clone();
            runtime.register_op("database.send", op_async(move |_state, batch: Vec<String>, _: ()| {
                let results = results.clone();
                async move {
                    results.produce_many(&mut batch.into_iter()).await;

                    Ok(())
                }
            }));
        }

        runtime.sync_ops_cache();
        runtime.execute_script("[native code]", include_str!("glue.js"))?;

        Ok(Self {
            db,
            mac,
            runtime,
            results,
            _i: PhantomData,
            _t: PhantomData
        })
    }

    pub async fn spawn(db: Arc<B>, script: oneshot::Receiver<String>) -> anyhow::Result<(JoinHandle<anyhow::Result<()>>, Arc<ResultsBuffer>)> {
        let (send, recv) = oneshot::channel();
        Ok((spawn_local(async move {
            let wrapper = Self::new(db)?;

            let mut runtime = wrapper.runtime;
            let results = wrapper.results;

            send.send(results.clone()).map_err(|_| anyhow!("could not send value"))?;
            
            let query = script.await?;
            runtime.execute_script("[query]", query.as_str())?;
            runtime.run_event_loop(false).await?;
            results.close().await;

            Ok(())
        }), recv.await?))
    }
}