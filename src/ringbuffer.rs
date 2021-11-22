use tokio::{sync::Mutex, task::yield_now};

pub struct RingBuffer<T: Sized, const S: usize> {
    buf: Mutex<[Option<T>; S]>,
    open: Mutex<bool>,
    writer_index: Mutex<usize>,
    reader_index: Mutex<usize>
}

impl<T, const S: usize> RingBuffer<T, S> {
    pub fn new() -> Self {
        Self {
            buf: Mutex::new([(); S].map(|_| Option::<T>::None)),
            open: Mutex::new(true),
            writer_index: Mutex::new(0),
            reader_index: Mutex::new(0)
        }
    }

    pub async fn close(&self) {
        *(self.open.lock().await) = false;
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
                    *index = (*index + 1) % S;
                    next = iter.next();
                    if next.is_none() {
                        return;
                    }
                }
            }
        }
    }

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
                    *index = (*index + 1) % S;
                    break o.take()
                } else {
                    let open = self.open.lock().await;
                    if !*open {
                        break None
                    } else {
                        yield_now().await;
                    }
                }
            }
        }
    }
}