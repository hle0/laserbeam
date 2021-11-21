use rocket::{get, launch, routes};

#[get("/")]
fn index() -> &'static str {
    "test"
}

#[launch]
fn rocket() -> _ {
    rocket::build().mount("/", routes![index])
}