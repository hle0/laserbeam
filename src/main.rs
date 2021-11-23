#![feature(try_blocks)]

use std::sync::Arc;

use database::JSONLinesDatabase;
use rocket::{post, routes, State};
use runtime::QueryRuntime;

mod database;
mod js;
mod runtime;

#[post("/query", data = "<script>")]
async fn query(script: String, db: &State<Arc<JSONLinesDatabase>>) -> String {
    let db = db.inner().clone();

    let result: Result<String, anyhow::Error> = tokio::task::spawn_blocking(move || {
        let mut values = Vec::new();

        let db = db.clone();

        for value in js::JSQueryRuntime::new().execute(script, db) {
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
    let mut args = std::env::args();
    args.next().unwrap();
    let file = match args.next() {
        Some(x) => x,
        None => "./db.jsonl".to_string(),
    };

    rocket::build()
        .manage(Arc::new(JSONLinesDatabase { path: file }))
        .mount("/", routes![query])
        .launch()
        .await
        .unwrap();
}
