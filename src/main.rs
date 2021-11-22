use rocket::{get, launch, routes};

mod js;
mod ringbuffer;

#[get("/")]
fn index() -> &'static str {
    "test"
}

#[launch]
fn rocket() -> _ {
    rocket::build().mount("/", routes![index])
}