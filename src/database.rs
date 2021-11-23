use std::{
    fs::File,
    io::{BufRead, BufReader},
};

pub trait Database: Sync + Send + 'static {
    fn get_table(
        &self,
        name: &str,
    ) -> anyhow::Result<Box<dyn Iterator<Item = crate::runtime::Value>>>;
}

#[derive(Clone)]
pub struct JSONLinesDatabase {
    pub path: String,
}

impl Database for JSONLinesDatabase {
    fn get_table(
        &self,
        _name: &str,
    ) -> anyhow::Result<Box<dyn Iterator<Item = crate::runtime::Value>>> {
        let mut reader = BufReader::new(File::open(&self.path)?);
        Ok(Box::new(std::iter::from_fn(move || {
            loop {
                let mut buf = String::new();
                let amount = reader.read_line(&mut buf).unwrap();

                if amount == 0 {
                    break None;
                } else if let Ok(value) = serde_json::from_str(buf.as_str()) {
                    /* TODO: don't silently ignore errors!! */
                    break Some(value);
                }
            }
        })))
    }
}
