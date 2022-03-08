/*
 * ScyllaDB, Rust and Actix Web
 *
 * MIT license
 *
 * Copyright (c) 2022 Dariusz Depta Engos Software
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

extern crate actix_cors;
extern crate actix_web;
extern crate scylla;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate tokio;

use crate::dto::{ResultDto, RowDto};
use crate::errors::*;
use crate::storage::Storage;
use actix_web::web::Json;
use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use std::sync::Mutex;

mod dto;
mod entity;
mod errors;
mod server;
mod storage;

pub struct AppData {
  storage: Mutex<Storage>,
}

#[get("/")]
async fn hello() -> impl Responder {
  HttpResponse::Ok().body("Hello world!")
}

#[get("/rows")]
async fn handler_get_rows(data: web::Data<AppData>) -> std::io::Result<Json<ResultDto<Vec<RowDto>>>> {
  if let Ok(storage) = data.storage.lock() {
    Ok(Json(ResultDto::data(get_rows(&storage).await)))
  } else {
    Ok(Json(ResultDto::error(err_no_access_to_storage())))
  }
}

async fn manual_hello() -> impl Responder {
  HttpResponse::Ok().body("Hey there!")
}

async fn get_rows(storage: &Storage) -> Vec<RowDto> {
  let mut result = vec![];
  if let Ok(a) = storage.get_rows().await {
    for r in a {
      result.push(RowDto { a: r.a, b: r.b, c: r.c })
    }
  }
  result
}

#[tokio::main]
async fn main() -> Result<()> {
  let mut storage = Storage::new();
  storage.connect().await?;
  storage.initialize().await?;
  let app_data = web::Data::new(AppData { storage: Mutex::new(storage) });
  let address = "0.0.0.0:8080";
  println!("started server {}", address);
  HttpServer::new(move || {
    App::new()
      .app_data(app_data.clone())
      .service(hello)
      .service(handler_get_rows)
      .route("/hey", web::get().to(manual_hello))
  })
  .bind(address)?
  .run()
  .await
  .map_err(err_internal)
}
