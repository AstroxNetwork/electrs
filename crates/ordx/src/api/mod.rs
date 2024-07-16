use std::net::SocketAddr;
use std::sync::Arc;

use axum::{Extension, http, Router};
use axum::body::Body;
use axum::http::{header, Response, StatusCode};
use axum::routing::{get, post};
use log::info;
use tower_governor::governor::GovernorConfigBuilder;
use tower_governor::GovernorLayer;
use tower_governor::key_extractor::SmartIpKeyExtractor;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::api::dto::R;
use crate::api::error::handle_panic;
use crate::db::RunesDB;
use crate::settings::Settings;

mod ip;
mod handler;
mod dto;
mod error;
mod util;
mod compat;

pub async fn create_server(settings: Arc<Settings>, runes_db: Arc<RunesDB>) -> anyhow::Result<()> {
    let governor_conf = Arc::new(
        GovernorConfigBuilder::default()
            .per_millisecond(settings.ip_limit_per_mills)
            .burst_size(settings.ip_limit_burst_size)
            .key_extractor(SmartIpKeyExtractor)
            .use_headers()
            .finish()
            .unwrap(),
    );
    let app = Router::new()
        .fallback(|uri: http::Uri| async move {
            let body: R<()> = R::error(-1, format!("No route: {}", &uri));
            let body = serde_json::to_string(&body).unwrap();
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap()
        })
        .route("/height", get(handler::block_height))
        .route("/rune/:id", get(handler::get_rune_by_id))
        .route("/runes", get(handler::paged_runes))
        .route("/runes/utxo/:address", get(compat::address_runes))
        .route("/runes/decode/psbt", post(handler::runes_decode_psbt))
        .route("/runes/decode/tx", post(handler::runes_decode_tx))
        .route("/runes/outputs", post(handler::outputs_runes))
        .route("/runes/ids", post(handler::get_runes_by_rune_ids))
        .route("/runes/address/:address", get(handler::address_runes))
        .layer(GovernorLayer {
            config: governor_conf,
        })
        .layer(CatchPanicLayer::custom(handle_panic))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .layer(Extension(runes_db))
        ;
    let listener = tokio::net::TcpListener::bind(&settings.api_host)
        .await?;
    info!("Listening on {}", settings.api_host);
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
        .await?;
    Ok(())
}
