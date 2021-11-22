#![feature(try_blocks)]

use database::JSONLinesStreamingBackend;
use rocket::{State, routes};

mod js;
mod ringbuffer;
mod database;
mod app;

#[tokio::main]
async fn main() {
    app::query("a".to_string(), &State::from(&JSONLinesStreamingBackend::new("a"))).await;
    rocket::build()
        .manage(app::DatabaseBackend::new("test.jsonl"))
        //.mount("/", routes![app::query])
        .launch()
        .await.unwrap();
}