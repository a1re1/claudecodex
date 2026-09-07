//! axum HTTP surface: the Anthropic Messages API as Claude Code speaks it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::anthropic::{ApiError, ErrorBody, MessagesRequest, StreamEvent};
use crate::models::Catalog;
use crate::translate::{self, ResponsesToAnthropic, SseParser};
use crate::upstream::{Upstream, UpstreamError};

/// Interval between SSE `ping` frames while the upstream is quiet.
const PING_INTERVAL: Duration = Duration::from_secs(15);
/// Give up on an upstream stream that has sent nothing for this long.
const MAX_STREAM_IDLE: Duration = Duration::from_secs(600);

#[derive(Clone)]
pub struct AppState {
    pub auth_token: Option<String>,
    pub upstream: Arc<Upstream>,
    pub catalog: Arc<Catalog>,
    pub default_effort: String,
}

pub fn router(state: AppState) -> Router {
    let api = Router::new()
        .route("/v1/models", get(list_models))
        .route("/v1/messages", post(messages))
        .route("/v1/messages/count_tokens", post(count_tokens))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));

    Router::new()
        .route("/api/hello", get(hello).head(hello))
        .merge(api)
        .layer(middleware::from_fn(log_request))
        .with_state(state)
}

fn error_response(status: StatusCode, kind: &str, message: impl Into<String>) -> Response {
    (status, Json(ErrorBody::new(kind, message))).into_response()
}

async fn log_request(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let started = std::time::Instant::now();
    let res = next.run(req).await;
    tracing::info!(
        %method,
        %path,
        status = res.status().as_u16(),
        ms = started.elapsed().as_millis() as u64,
        "request"
    );
    res
}

async fn require_auth(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let Some(expected) = state.auth_token.as_deref() else {
        return next.run(req).await;
    };
    let headers = req.headers();
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
        })
        .map(str::trim);
    let api_key = headers.get("x-api-key").and_then(|v| v.to_str().ok());
    if bearer == Some(expected) || api_key == Some(expected) {
        next.run(req).await
    } else {
        error_response(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "invalid or missing auth token",
        )
    }
}

async fn hello() -> StatusCode {
    StatusCode::OK
}

async fn list_models(State(state): State<AppState>) -> Response {
    let models = state.catalog.public_models().await;
    let first_id = models.first().map(|m| m.id.clone());
    let last_id = models.last().map(|m| m.id.clone());
    Json(json!({
        "data": models,
        "has_more": false,
        "first_id": first_id,
        "last_id": last_id,
    }))
    .into_response()
}

async fn count_tokens(Json(body): Json<Value>) -> Response {
    // Rough estimate: the upstream exposes no token counter for the Codex backend.
    let chars = body.to_string().len() as u64;
    Json(json!({ "input_tokens": chars.div_ceil(4) })).into_response()
}

fn upstream_error_response(err: UpstreamError) -> Response {
    match err {
        UpstreamError::Status { status, body } => {
            let message = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| {
                    v.pointer("/error/message")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .unwrap_or_else(|| body.chars().take(2000).collect());
            let kind = match status {
                StatusCode::TOO_MANY_REQUESTS => "rate_limit_error",
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => "authentication_error",
                StatusCode::BAD_REQUEST => "invalid_request_error",
                s if s.is_server_error() => "api_error",
                _ => "api_error",
            };
            error_response(status, kind, message)
        }
        UpstreamError::Other(e) => error_response(
            StatusCode::BAD_GATEWAY,
            "api_error",
            format!("upstream: {e:#}"),
        ),
    }
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

async fn messages(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let req: MessagesRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!("malformed request body: {e}"),
            )
        }
    };

    let Some(resolved) = state.catalog.resolve(&req.model).await else {
        return error_response(
            StatusCode::NOT_FOUND,
            "not_found_error",
            format!("model not found: {}", req.model),
        );
    };
    let upstream_model = state.catalog.model(&resolved.slug).await;
    let effort = translate::choose_effort(
        resolved.effort.as_deref(),
        req.thinking.as_ref(),
        &state.default_effort,
        upstream_model.as_ref(),
    );
    let session_id = header_str(&headers, "x-claude-code-session-id");
    let upstream_req = translate::anthropic_to_responses(&req, &resolved.slug, &effort, session_id);
    tracing::debug!(
        model = %resolved.slug,
        %effort,
        stream = req.stream,
        tools = req.tools.len(),
        input_items = upstream_req.input.len(),
        "forwarding to codex"
    );

    let stream = match state.upstream.responses(&upstream_req).await {
        Ok(s) => s,
        Err(e) => return upstream_error_response(e),
    };
    let mut extra_headers = Vec::new();
    for (name, value) in &stream.headers {
        if let (Ok(n), Ok(v)) = (
            header::HeaderName::from_bytes(name.as_bytes()),
            header::HeaderValue::from_str(value),
        ) {
            extra_headers.push((n, v));
        }
    }

    let public_model = req.model.clone();
    if req.stream {
        let (tx, rx) = mpsc::channel::<String>(64);
        tokio::spawn(pump_stream(stream.body, public_model, tx));
        let body =
            Body::from_stream(ReceiverStream::new(rx).map(Ok::<_, std::convert::Infallible>));
        let mut res = Response::new(body);
        let h = res.headers_mut();
        h.insert(
            header::CONTENT_TYPE,
            "text/event-stream".parse().expect("static"),
        );
        h.insert(header::CACHE_CONTROL, "no-cache".parse().expect("static"));
        for (n, v) in extra_headers {
            h.insert(n, v);
        }
        res
    } else {
        let events = collect_events(stream.body, public_model.clone()).await;
        match translate::assemble(public_model, &events) {
            Ok(msg) => {
                let mut res = Json(msg).into_response();
                for (n, v) in extra_headers {
                    res.headers_mut().insert(n, v);
                }
                res
            }
            Err(ApiError { kind, message }) => {
                let status = match kind.as_str() {
                    "rate_limit_error" => StatusCode::TOO_MANY_REQUESTS,
                    "invalid_request_error" => StatusCode::BAD_REQUEST,
                    _ => StatusCode::BAD_GATEWAY,
                };
                error_response(status, &kind, message)
            }
        }
    }
}

type UpstreamBody =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = reqwest::Result<bytes::Bytes>> + Send>>;

/// Drives the upstream SSE body through the translator, writing Anthropic SSE frames to `tx`
/// and interleaving `ping` frames while the upstream is silent.
async fn pump_stream(mut body: UpstreamBody, model: String, tx: mpsc::Sender<String>) {
    let mut parser = SseParser::default();
    let mut translator = ResponsesToAnthropic::new(model);
    let ping = StreamEvent::Ping.to_sse();
    let mut last_activity = Instant::now();
    loop {
        let next = tokio::time::timeout(PING_INTERVAL, body.next()).await;
        match next {
            Err(_elapsed) => {
                if last_activity.elapsed() >= MAX_STREAM_IDLE {
                    tracing::warn!("upstream stream idle for {MAX_STREAM_IDLE:?}; aborting");
                    let ev = StreamEvent::Error {
                        error: ApiError {
                            kind: "api_error".into(),
                            message: "upstream stream idle timeout".into(),
                        },
                    };
                    let _ = tx.send(ev.to_sse()).await;
                    return;
                }
                if tx.send(ping.clone()).await.is_err() {
                    return;
                }
            }
            Ok(Some(Ok(chunk))) => {
                last_activity = Instant::now();
                for frame in parser.push(&chunk) {
                    let Ok(value) = serde_json::from_str::<Value>(&frame.data) else {
                        tracing::debug!(event = ?frame.event, "non-json sse frame ignored");
                        continue;
                    };
                    for ev in translator.handle(&value) {
                        if tx.send(ev.to_sse()).await.is_err() {
                            return;
                        }
                    }
                }
                if translator.finished() {
                    return;
                }
            }
            Ok(Some(Err(e))) => {
                tracing::warn!(error = %e, "upstream stream error");
                let ev = StreamEvent::Error {
                    error: ApiError {
                        kind: "api_error".into(),
                        message: format!("upstream stream error: {e}"),
                    },
                };
                let _ = tx.send(ev.to_sse()).await;
                return;
            }
            Ok(None) => {
                for ev in translator.end_of_stream() {
                    if tx.send(ev.to_sse()).await.is_err() {
                        return;
                    }
                }
                return;
            }
        }
    }
}

async fn collect_events(mut body: UpstreamBody, model: String) -> Vec<StreamEvent> {
    let mut parser = SseParser::default();
    let mut translator = ResponsesToAnthropic::new(model);
    let mut events = Vec::new();
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(chunk) => {
                for frame in parser.push(&chunk) {
                    if let Ok(value) = serde_json::from_str::<Value>(&frame.data) {
                        events.extend(translator.handle(&value));
                    }
                }
                if translator.finished() {
                    return events;
                }
            }
            Err(e) => {
                events.push(StreamEvent::Error {
                    error: ApiError {
                        kind: "api_error".into(),
                        message: format!("upstream stream error: {e}"),
                    },
                });
                return events;
            }
        }
    }
    events.extend(translator.end_of_stream());
    events
}
