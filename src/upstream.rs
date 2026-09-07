//! HTTP client for the Codex backend (`chatgpt.com/backend-api/codex`).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::Stream;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::auth::{AuthStore, Credentials};

pub const DEFAULT_BASE: &str = "https://chatgpt.com/backend-api/codex";
const CLIENT_VERSION: &str = "0.153.4";
const ORIGINATOR: &str = "codex_cli_rs";

/// Response headers worth surfacing to the client (rate-limit telemetry).
pub const FORWARDED_HEADERS: &[&str] = &[
    "x-codex-primary-used-percent",
    "x-codex-primary-reset-after-seconds",
    "x-codex-plan-type",
];

#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    #[error("upstream returned HTTP {status}: {body}")]
    Status { status: StatusCode, body: String },
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReasoningLevel {
    #[serde(default)]
    pub effort: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamModel {
    pub slug: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub default_reasoning_level: Option<String>,
    #[serde(default)]
    pub supported_reasoning_levels: Vec<ReasoningLevel>,
}

#[derive(Debug, Deserialize)]
struct ModelsResponse {
    #[serde(default)]
    models: Vec<UpstreamModel>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Reasoning {
    pub effort: String,
    pub summary: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct TextOptions {
    pub verbosity: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResponsesRequest {
    pub model: String,
    pub instructions: String,
    pub input: Vec<Value>,
    pub tools: Vec<Value>,
    pub tool_choice: Value,
    pub parallel_tool_calls: bool,
    pub reasoning: Reasoning,
    pub store: bool,
    pub stream: bool,
    pub include: Vec<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<TextOptions>,
}

pub struct ResponsesStream {
    pub headers: Vec<(String, String)>,
    pub body: std::pin::Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>,
}

pub struct Upstream {
    base: String,
    http: reqwest::Client,
    auth: Arc<AuthStore>,
    session_id: String,
}

impl Upstream {
    pub fn new(base: String, auth: Arc<AuthStore>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(20))
            .build()
            .context("build http client")?;
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            http,
            auth,
            session_id: uuid::Uuid::new_v4().to_string(),
        })
    }

    fn headers(&self, creds: &Credentials) -> Result<HeaderMap> {
        let mut h = HeaderMap::new();
        let mut bearer = HeaderValue::from_str(&format!("Bearer {}", creds.access_token))
            .context("access token is not a valid header value")?;
        bearer.set_sensitive(true);
        h.insert(AUTHORIZATION, bearer);
        h.insert(
            "chatgpt-account-id",
            HeaderValue::from_str(&creds.account_id).context("account id header")?,
        );
        h.insert("originator", HeaderValue::from_static(ORIGINATOR));
        h.insert(
            USER_AGENT,
            HeaderValue::from_static(concat!("codex_cli_rs/", "0.153.4")),
        );
        h.insert(
            "OpenAI-Beta",
            HeaderValue::from_static("responses=experimental"),
        );
        h.insert("version", HeaderValue::from_static(CLIENT_VERSION));
        h.insert(
            "session-id",
            HeaderValue::from_str(&self.session_id).context("session id header")?,
        );
        h.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(h)
    }

    /// Fetch the model catalogue. Cheap enough to call on demand; cached by `models::Catalog`.
    pub async fn list_models(
        &self,
        timeout: Duration,
    ) -> Result<Vec<UpstreamModel>, UpstreamError> {
        let url = format!("{}/models?client_version={}", self.base, CLIENT_VERSION);
        let mut creds = self.auth.credentials().await?;
        for attempt in 0..2 {
            let resp = self
                .http
                .get(&url)
                .headers(self.headers(&creds)?)
                .timeout(timeout)
                .send()
                .await
                .context("GET /models")?;
            let status = resp.status();
            if status == StatusCode::UNAUTHORIZED && attempt == 0 {
                creds = self.auth.refresh_after_401(&creds.access_token).await?;
                continue;
            }
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(UpstreamError::Status { status, body });
            }
            let parsed: ModelsResponse = resp.json().await.context("parse /models")?;
            return Ok(parsed.models);
        }
        Err(anyhow::anyhow!("unreachable: /models retry loop exhausted").into())
    }

    /// Start a streaming `/responses` call. Non-2xx statuses are returned as errors with the
    /// body; a 401 triggers one token refresh and one retry.
    pub async fn responses(
        &self,
        req: &ResponsesRequest,
    ) -> Result<ResponsesStream, UpstreamError> {
        let url = format!("{}/responses", self.base);
        let mut creds = self.auth.credentials().await?;
        for attempt in 0..2 {
            let mut headers = self.headers(&creds)?;
            headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
            headers.insert(
                "x-client-request-id",
                HeaderValue::from_str(&uuid::Uuid::new_v4().to_string())
                    .context("request id header")?,
            );
            let resp = self
                .http
                .post(&url)
                .headers(headers)
                .json(req)
                .send()
                .await
                .context("POST /responses")?;
            let status = resp.status();
            if status == StatusCode::UNAUTHORIZED && attempt == 0 {
                tracing::info!("upstream 401; refreshing token and retrying once");
                creds = self.auth.refresh_after_401(&creds.access_token).await?;
                continue;
            }
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(UpstreamError::Status { status, body });
            }
            let headers = FORWARDED_HEADERS
                .iter()
                .filter_map(|name| {
                    resp.headers()
                        .get(*name)
                        .and_then(|v| v.to_str().ok())
                        .map(|v| (name.to_string(), v.to_string()))
                })
                .collect();
            return Ok(ResponsesStream {
                headers,
                body: Box::pin(resp.bytes_stream()),
            });
        }
        Err(anyhow::anyhow!("unreachable: /responses retry loop exhausted").into())
    }
}
