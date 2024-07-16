use std::any::Any;

use axum::body::Bytes;
use axum::http;
use axum::http::{header, StatusCode};
use axum::response::Response;
use http_body_util::Full;
use crate::api::dto::R;

pub fn handle_panic(err: Box<dyn Any + Send + 'static>) -> http::Response<Full<Bytes>> {
    let details = if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else {
        "Unknown error".to_string()
    };

    let body: R<()> = R::error(-1, details);
    let body = serde_json::to_string(&body).unwrap();

    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::from(body))
        .unwrap()
}
