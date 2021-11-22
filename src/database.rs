use async_trait::async_trait;
use std::{io::SeekFrom, path::PathBuf};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncSeekExt, BufReader},
};

use crate::js::BatchedStreamProvider;

#[derive(Clone)]
pub struct JSONLinesStreamingBackend {
    pub path: PathBuf,
}

const STREAMED_DATA_SIZE: usize = 65_536;

impl JSONLinesStreamingBackend {
    pub fn new(s: &str) -> Self {
        Self {
            path: PathBuf::from(s),
        }
    }
}

#[async_trait]
impl BatchedStreamProvider<u64, String> for JSONLinesStreamingBackend {
    async fn begin(&self) -> anyhow::Result<u64> {
        Ok(0)
    }

    async fn next(&self, pos: &mut u64) -> anyhow::Result<Vec<String>> {
        let mut file = File::open(&self.path).await?;
        file.seek(SeekFrom::Start(*pos)).await?;
        let mut reader = BufReader::new(file);

        let mut s = String::new();
        while s.len() < STREAMED_DATA_SIZE {
            let read = reader.read_line(&mut s).await?;
            if read == 0 {
                break;
            } else {
                *pos += read as u64;
            }
        }

        Ok(s.split('\n')
            .filter(|x| !x.trim().is_empty())
            .map(|x| x.to_string())
            .collect::<Vec<String>>())
    }

    async fn more(&self, pos: &u64) -> anyhow::Result<bool> {
        let mut file = File::open(&self.path).await?;
        file.seek(SeekFrom::Start(*pos)).await?;
        let mut reader = BufReader::new(file);

        let mut s = String::new();
        Ok(reader.read_line(&mut s).await? > 0)
    }
}
