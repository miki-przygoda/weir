use crate::wab::{self, WabError};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::services::ServeDir;

#[derive(Clone)]
pub struct AppState {
    pub wab_dir: Arc<PathBuf>,
    pub static_dir: Arc<PathBuf>,
}

fn err_response(e: WabError) -> Response {
    let code = match e {
        WabError::BadPath(_) => StatusCode::BAD_REQUEST,
        WabError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (code, Json(serde_json::json!({ "error": e.to_string() }))).into_response()
}

#[derive(Deserialize)]
struct SegQuery {
    path: String,
    #[serde(default)]
    offset: usize,
    #[serde(default = "def_limit")]
    limit: usize,
}
fn def_limit() -> usize {
    100
}
#[derive(Deserialize)]
struct PathQuery {
    path: String,
}

pub fn router(wab_dir: PathBuf) -> Router {
    router_with_static(wab_dir, default_static_dir())
}

pub fn router_with_static(wab_dir: PathBuf, static_dir: PathBuf) -> Router {
    let state = AppState {
        wab_dir: Arc::new(wab_dir),
        static_dir: Arc::new(static_dir.clone()),
    };
    Router::new()
        .route(
            "/api/wab/segments",
            get(|State(s): State<AppState>| async move {
                match wab::inventory(&s.wab_dir) {
                    Ok(r) => Json(r).into_response(),
                    Err(e) => err_response(e),
                }
            }),
        )
        .route(
            "/api/wab/segment",
            get(
                |State(s): State<AppState>, Query(q): Query<SegQuery>| async move {
                    match wab::records(&s.wab_dir, &q.path, q.offset, q.limit) {
                        Ok(r) => Json(r).into_response(),
                        Err(e) => err_response(e),
                    }
                },
            ),
        )
        .route(
            "/api/wab/dead-letter",
            get(|State(s): State<AppState>| async move {
                match wab::dead_letter(&s.wab_dir) {
                    Ok(r) => Json(r).into_response(),
                    Err(e) => err_response(e),
                }
            }),
        )
        .route(
            "/api/wab/verify",
            get(
                |State(s): State<AppState>, Query(q): Query<PathQuery>| async move {
                    match wab::verify(&s.wab_dir, &q.path) {
                        Ok(r) => Json(r).into_response(),
                        Err(e) => err_response(e),
                    }
                },
            ),
        )
        .fallback_service(ServeDir::new(static_dir).append_index_html_on_directories(true))
        .with_state(state)
}

fn default_static_dir() -> PathBuf {
    // ../static relative to the crate (works from `cargo run` in the workspace).
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static")
}
