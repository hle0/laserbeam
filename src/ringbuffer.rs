use tokio::{
    select,
    sync::{watch, Mutex},
    task::yield_now,
};

pub struct RingBuffer<T: Sized, const S: usize> {
    buf: Mutex<[Option<T>; S]>,
    open: (watch::Sender<bool>, Mutex<watch::Receiver<bool>>),
    writer_index: Mutex<usize>,
    reader_index: Mutex<usize>,
}

impl<T, const S: usize> RingBuffer<T, S> {
    pub fn new() -> Self {
        let (send, recv) = watch::channel(true);
        Self {
            buf: Mutex::new([(); S].map(|_| Option::<T>::None)),
            open: (send, Mutex::new(recv)),
            writer_index: Mutex::new(0),
            reader_index: Mutex::new(0),
        }
    }

    pub fn close(&self) {
        drop(self.open.0.send(false));
    }

    pub async fn produce_many<I: Iterator<Item = T>>(&self, iter: &mut I) {
        let mut next = iter.next();
        if next.is_none() {
            return;
        }

        {
            let mut index = self.writer_index.lock().await;
            {
                let mut buf = self.buf.lock().await;
                while buf[*index].is_none() {
                    buf[*index] = next;
                    *index += 1;
                    *index %= S;
                    next = iter.next();
                    if next.is_none() {
                        return;
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    pub async fn produce_one(&self, item: T) {
        self.produce_many(&mut std::iter::once(item)).await
    }

    pub async fn consume_one(&self) -> Option<T> {
        loop {
            let mut index = self.reader_index.lock().await;
            {
                let mut buf = self.buf.lock().await;
                let o = &mut buf[*index];
                if o.is_some() {
                    *index += 1;
                    *index %= S;
                    break o.take();
                } else {
                    let mut open = self.open.1.lock().await;
                    select! {
                        _ = open.changed() => {}
                        _ = yield_now() => {}
                    };

                    if !*open.borrow() {
                        break None;
                    }
                }
            }
        }
    }
}
