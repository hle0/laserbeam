#![feature(try_blocks)]

use std::sync::Arc;

use rocket::routes;

mod js;

#[tokio::main]
async fn main() {
    let mut args = std::env::args();
    args.next().unwrap();
    let file = match args.next() {
        Some(x) => x,
        None => "./db.jsonl".to_string(),
    };

    rocket::build()
        .manage(Arc::new(app::DatabaseBackend::new(&file)))
        .mount("/", routes![app::query])
        .launch()
        .await
        .unwrap();
}
