use std::sync::Arc;

use serde::Deserialize;

use crate::database::Database;

pub type Value = serde_json::Value;

/// Execution limits placed on scripts running in the runtime.
/// 
/// Not all runtimes support all of these limits - some may support only one, or some may support none of them.
#[derive(Clone, Copy, Default, Deserialize)]
pub struct Limits {
    /// The size, in bytes, allowed for heap allocations.
    /// 
    /// The runtime may allocate very slightly more than this for book-keeping purposes, etc.
    pub heap: Option<usize>,

    /// The maximum amount of time the script is allowed to run before it is forcefully killed, in seconds.
    /// 
    /// This may or may not include the time needed for the runtime to spin up, so calls to [`QueryRuntime::execute`]
    /// may take slightly longer than this time.
    pub time: Option<f64>
}

/// A runtime for executing a query.
/// This is/was only [`crate::js::JSQueryRuntime`], but theoretically other runtimes could be used that support different languages.
pub trait QueryRuntime {
    /// Attempt to set the execution limits for each script in the runtime.
    /// 
    /// Since not all limits are supported by all runtimes, this function returns what the limits were actually set to.
    fn set_limits(&mut self, limits: &Limits) -> &Limits;

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
