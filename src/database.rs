use std::{
    fs::File,
    io::{BufRead, BufReader},
};

/// A thread-safe database handle.
///
/// This could store read-only data in memory, query another external database, or generate the data itself.
/// In practice, most `Database` implementations will query another database or a local file.
pub trait Database: Sync + Send + 'static {
    /// Given the name of a table, try to create an iterator from the contents of the table.
    ///
    /// If the table exists and can be accessed, returns a `Box<Iterator<...>>` of the rows of the table.
    /// Otherwise, returns an `anyhow::Error`.
    fn get_table(
        &self,
        name: &str,
    ) -> anyhow::Result<Box<dyn Iterator<Item = crate::runtime::Value>>>;
}

/// A Database which gets data by reading lines from a single file where each line is a JSON object.
#[derive(Clone)]
pub struct JSONLinesDatabase {
    /// The path to the JSON-lines file.
    /// Currently, every "table" is just all the rows read from this file; there is no concept of tables.
    pub path: String,
}

impl JSONLinesDatabase {
    /// Generate a new [`JSONLinesDatabase`] from the given `path`.
    pub const fn new(path: String) -> Self {
        Self { path }
    }
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
