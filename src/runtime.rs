use std::sync::Arc;

use crate::database::Database;

pub type Value = serde_json::Value;

pub trait QueryRuntime {
    fn execute(
        &self,
        script: String,
        database: Arc<dyn Database>,
    ) -> Box<dyn Iterator<Item = anyhow::Result<Value>>>;
}
