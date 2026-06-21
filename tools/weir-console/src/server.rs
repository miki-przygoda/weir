use crate::ops::{self, OpsConfig};
use crate::wab::{self, WabError};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::services::ServeDir;

#[derive(Clone)]
pub struct AppState {
    pub wab_dir: Arc<PathBuf>,
    pub static_dir: Arc<PathBuf>,
    pub ops: Arc<OpsConfig>,
}

fn err_response(e: WabError) -> Response {
    let code = match e {
        WabError::BadPath(_) => StatusCode::BAD_REQUEST,
        WabError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (code, Json(serde_json::json!({ "error": e.to_string() }))).into_response()
}

fn ops_err_response(e: ops::OpsError) -> Response {
    let code = match e {
        ops::OpsError::ReadOnly => StatusCode::FORBIDDEN,
        ops::OpsError::CtlFailed(_) => StatusCode::BAD_GATEWAY,
        ops::OpsError::NotFound(_) | ops::OpsError::BadOutput(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (code, Json(json!({ "error": e.to_string() }))).into_response()
}

fn ops_json(r: Result<serde_json::Value, ops::OpsError>) -> Response {
    match r {
        Ok(v) => Json(v).into_response(),
        Err(e) => ops_err_response(e),
    }
}

#[derive(Deserialize)]
struct RequeueQuery {
    #[serde(default)]
    durability: Option<String>,
}

async fn requeue_handler(ops_cfg: &OpsConfig, q: RequeueQuery, commit: bool) -> Response {
    let d = q.durability.as_deref().unwrap_or("batched");
    let Some(dur) = ops::Durability::parse(d) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("invalid durability {d:?} (expected sync|batched|buffered)") })),
        )
            .into_response();
    };
    ops_json(ops::requeue(ops_cfg, dur, commit).await)
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
    let ops = default_ops(&wab_dir);
    router_with_ops(wab_dir, default_static_dir(), ops)
}

pub fn router_with_static(wab_dir: PathBuf, static_dir: PathBuf) -> Router {
    let ops = default_ops(&wab_dir);
    router_with_ops(wab_dir, static_dir, ops)
}

/// A default ops config (weir-ctl on PATH, weir-ctl/daemon defaults). Used by the
/// Explorer-only constructors; `main`/tests inject a real one via `router_with_ops`.
fn default_ops(wab_dir: &PathBuf) -> OpsConfig {
    OpsConfig {
        weir_ctl: PathBuf::from("weir-ctl"),
        wab_dir: wab_dir.clone(),
        metrics_addr: "127.0.0.1:9185".to_string(),
        socket: PathBuf::from("/run/weir/weir.sock"),
        read_only: false,
    }
}

pub fn router_with_ops(wab_dir: PathBuf, static_dir: PathBuf, ops: OpsConfig) -> Router {
    let state = AppState {
        wab_dir: Arc::new(wab_dir),
        static_dir: Arc::new(static_dir.clone()),
        ops: Arc::new(ops),
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
            get(|State(s): State<AppState>, Query(q): Query<SegQuery>| async move {
                match wab::records(&s.wab_dir, &q.path, q.offset, q.limit) {
                    Ok(r) => Json(r).into_response(),
                    Err(e) => err_response(e),
                }
            }),
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
            get(|State(s): State<AppState>, Query(q): Query<PathQuery>| async move {
                match wab::verify(&s.wab_dir, &q.path) {
                    Ok(r) => Json(r).into_response(),
                    Err(e) => err_response(e),
                }
            }),
        )
        .route(
            "/api/ops/status",
            get(|State(s): State<AppState>| async move { ops_json(ops::status(&s.ops).await) }),
        )
        .route(
            "/api/ops/dead-letter",
            get(|State(s): State<AppState>| async move { ops_json(ops::dead_letter(&s.ops).await) }),
        )
        .route(
            "/api/ops/requeue/preview",
            post(|State(s): State<AppState>, Query(q): Query<RequeueQuery>| async move {
                requeue_handler(&s.ops, q, false).await
            }),
        )
        .route(
            "/api/ops/requeue",
            post(|State(s): State<AppState>, Query(q): Query<RequeueQuery>| async move {
                requeue_handler(&s.ops, q, true).await
            }),
        )
        .route(
            "/api/ops/drop/preview",
            post(|State(s): State<AppState>| async move { ops_json(ops::drop_dl(&s.ops, false).await) }),
        )
        .route(
            "/api/ops/drop",
            post(|State(s): State<AppState>| async move { ops_json(ops::drop_dl(&s.ops, true).await) }),
        )
        .fallback_service(ServeDir::new(static_dir).append_index_html_on_directories(true))
        .with_state(state)
}

fn default_static_dir() -> PathBuf {
    // ../static relative to the crate (works from `cargo run` in the workspace).
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static")
}
