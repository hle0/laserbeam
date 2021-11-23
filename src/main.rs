#![feature(try_blocks)]

use std::sync::Arc;

use database::JSONLinesDatabase;
use rocket::{State, fairing::AdHoc, post, routes};
use runtime::QueryRuntime;

use crate::config::Config;

mod database;
mod js;
mod runtime;
mod config;

/// The main query endpoint.
/// Currently, this just runs a script supplied via POST data in the [`js::JSQueryRuntime`].
#[post("/query", data = "<script>")]
async fn query(script: String, db: &State<Arc<JSONLinesDatabase>>, config: &State<Config>) -> String {
    let db = db.inner().clone();
    let config = config.inner().clone();

    let result: Result<String, anyhow::Error> = tokio::task::spawn_blocking(move || {
        let mut values = Vec::new();

        let db = db.clone();

        let mut runtime = js::JSQueryRuntime::new();
        runtime.set_limits(&config.app.runtime_limits);
        for value in runtime.execute(script, db) {
            values.push(value?);
        }

        Ok(serde_json::to_string(&values)?)
    })
    .await
    .unwrap();

    match result {
        Ok(o) => o,
        Err(e) => e.to_string(),
    }
}

#[tokio::main]
async fn main() {
    let build = rocket::build()
        .attach(AdHoc::config::<Config>());
    
    let config: Config = build.figment().extract().expect("config");

    build.manage(Arc::new(JSONLinesDatabase::new(config.app.database)))
        .mount("/", routes![query])
        .launch()
        .await
        .unwrap();
}
