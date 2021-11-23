use std::sync::Arc;

use crate::database::Database;

pub type Value = serde_json::Value;

/// A runtime for executing a query.
/// This is/was only [`crate::js::JSQueryRuntime`], but theoretically other runtimes could be used that support different languages.
pub trait QueryRuntime {
    /// Execute a query using the given database.
    ///
    /// For some runtimes, this may create the entire execution context, use it, and then drop it after the returned iterator ends;
    /// For others, it may reuse the execution context across multiple scripts.
    ///
    /// `script` is the actual query - e.g. a JavaScript script for [`crate::js::JSQueryRuntime`].
    /// `database` is the database that the script will have access to. How this is accessed from the script depends on the runtime.
    ///
    /// The returned boxed iterator will yield results (usually as they are generated) from the runtime;
    /// The iterator will generate results until the script returns, or the runtime generates an error.
    /// If the runtime generates an error, it will always be the last item in the iterator;
    /// additionally, the iterator will never produce more than one error.
    /// If no errors occur, the last item in the iterator will be an `Ok(...)`.
    fn execute(
        &self,
        script: String,
        database: Arc<dyn Database>,
    ) -> Box<dyn Iterator<Item = anyhow::Result<Value>>>;
}
