//! Model alias layer: Codex slugs are exposed as `claude-<slug>[:<effort>]` so Claude Code's
//! gateway model discovery (which keeps only ids containing "claude") lists them.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::{Mutex, RwLock};

use crate::upstream::{Upstream, UpstreamModel};

const PREFIX: &str = "claude-";
const CACHE_TTL: Duration = Duration::from_secs(600);
const UPSTREAM_TIMEOUT: Duration = Duration::from_millis(2500);

#[derive(Debug, Clone, Serialize)]
pub struct PublicModel {
    pub id: String,
    pub display_name: String,
    pub description: String,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub created_at: String,
}

/// A public model id resolved to an upstream slug plus optional reasoning effort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub slug: String,
    pub effort: Option<String>,
}

/// Split a public id into `(slug, effort)` without consulting the catalogue.
/// `claude-gpt-5.5:high` → ("gpt-5.5", Some("high")); `gpt-5.5` → ("gpt-5.5", None).
pub fn parse_id(id: &str) -> Resolved {
    let bare = id.strip_prefix(PREFIX).unwrap_or(id);
    match bare.rsplit_once(':') {
        Some((slug, effort)) if !effort.is_empty() && !slug.is_empty() => Resolved {
            slug: slug.to_string(),
            effort: Some(effort.to_string()),
        },
        _ => Resolved {
            slug: bare.to_string(),
            effort: None,
        },
    }
}

pub fn public_id(slug: &str, effort: Option<&str>) -> String {
    match effort {
        Some(e) => format!("{PREFIX}{slug}:{e}"),
        None => format!("{PREFIX}{slug}"),
    }
}

/// Resolve against a known model list. Returns None when the slug is unknown or the effort
/// suffix is not one the model supports.
pub fn resolve_in(models: &[UpstreamModel], id: &str) -> Option<Resolved> {
    let parsed = parse_id(id);
    let model = models.iter().find(|m| m.slug == parsed.slug)?;
    if let Some(effort) = &parsed.effort {
        let supported = model
            .supported_reasoning_levels
            .iter()
            .any(|l| &l.effort == effort);
        if !supported {
            return None;
        }
    }
    Some(parsed)
}

pub fn public_list(models: &[UpstreamModel], created_at: &str) -> Vec<PublicModel> {
    let mut out = Vec::new();
    for m in models {
        let display = if m.display_name.is_empty() {
            m.slug.clone()
        } else {
            m.display_name.clone()
        };
        out.push(PublicModel {
            id: public_id(&m.slug, None),
            display_name: display.clone(),
            description: m.description.clone(),
            kind: "model",
            created_at: created_at.to_string(),
        });
        for level in &m.supported_reasoning_levels {
            if level.effort.is_empty() {
                continue;
            }
            out.push(PublicModel {
                id: public_id(&m.slug, Some(&level.effort)),
                display_name: format!("{display} ({})", level.effort),
                description: if level.description.is_empty() {
                    m.description.clone()
                } else {
                    format!("{} — {}", m.description, level.description)
                },
                kind: "model",
                created_at: created_at.to_string(),
            });
        }
    }
    out
}

struct Cached {
    models: Vec<UpstreamModel>,
    fetched: Instant,
}

pub struct Catalog {
    upstream: Arc<Upstream>,
    cache: RwLock<Option<Cached>>,
    /// Serialises upstream refreshes so a cold or expired cache does not fan
    /// out into one `/models` call per concurrent request.
    refresh: Mutex<()>,
    started_at: String,
}

impl Catalog {
    pub fn new(upstream: Arc<Upstream>) -> Self {
        Self {
            upstream,
            cache: RwLock::new(None),
            refresh: Mutex::new(()),
            started_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        }
    }

    /// Current model list: fresh cache, else upstream, else stale cache, else empty.
    pub async fn models(&self) -> Vec<UpstreamModel> {
        if let Some(models) = self.fresh().await {
            return models;
        }
        let _guard = self.refresh.lock().await;
        // Another request may have refreshed while we waited for the guard.
        if let Some(models) = self.fresh().await {
            return models;
        }
        match self.upstream.list_models(UPSTREAM_TIMEOUT).await {
            Ok(models) if !models.is_empty() => {
                *self.cache.write().await = Some(Cached {
                    models: models.clone(),
                    fetched: Instant::now(),
                });
                models
            }
            Ok(_) => {
                tracing::warn!("upstream returned an empty model list");
                self.stale().await
            }
            Err(e) => {
                tracing::warn!(error = %e, "model list fetch failed; serving cached list");
                self.stale().await
            }
        }
    }

    async fn fresh(&self) -> Option<Vec<UpstreamModel>> {
        self.cache
            .read()
            .await
            .as_ref()
            .filter(|c| c.fetched.elapsed() < CACHE_TTL)
            .map(|c| c.models.clone())
    }

    async fn stale(&self) -> Vec<UpstreamModel> {
        self.cache
            .read()
            .await
            .as_ref()
            .map(|c| c.models.clone())
            .unwrap_or_default()
    }

    pub async fn public_models(&self) -> Vec<PublicModel> {
        public_list(&self.models().await, &self.started_at)
    }

    pub async fn resolve(&self, id: &str) -> Option<Resolved> {
        resolve_in(&self.models().await, id)
    }

    /// Upstream metadata for a resolved slug, if the catalog knows it.
    pub async fn model(&self, slug: &str) -> Option<UpstreamModel> {
        self.models().await.into_iter().find(|m| m.slug == slug)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::ReasoningLevel;

    fn fixture() -> Vec<UpstreamModel> {
        vec![UpstreamModel {
            slug: "gpt-5.5".into(),
            display_name: "GPT-5.5".into(),
            description: "General model".into(),
            default_reasoning_level: Some("medium".into()),
            supported_reasoning_levels: vec![
                ReasoningLevel {
                    effort: "low".into(),
                    description: "fast".into(),
                },
                ReasoningLevel {
                    effort: "high".into(),
                    description: "".into(),
                },
            ],
        }]
    }

    #[test]
    fn parse_strips_prefix_and_effort() {
        assert_eq!(
            parse_id("claude-gpt-5.5:high"),
            Resolved {
                slug: "gpt-5.5".into(),
                effort: Some("high".into())
            }
        );
        assert_eq!(
            parse_id("gpt-5.5"),
            Resolved {
                slug: "gpt-5.5".into(),
                effort: None
            }
        );
        assert_eq!(parse_id("claude-gpt-5.5:").slug, "gpt-5.5:");
    }

    #[test]
    fn alias_round_trip() {
        let list = public_list(&fixture(), "2026-01-01T00:00:00Z");
        let ids: Vec<&str> = list.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                "claude-gpt-5.5",
                "claude-gpt-5.5:low",
                "claude-gpt-5.5:high"
            ]
        );
        assert_eq!(list[1].display_name, "GPT-5.5 (low)");
        assert_eq!(list[2].description, "General model");
        for m in &list {
            assert!(resolve_in(&fixture(), &m.id).is_some(), "{}", m.id);
        }
    }

    #[test]
    fn resolve_rejects_unknown() {
        assert!(resolve_in(&fixture(), "claude-gpt-5.5:xhigh").is_none());
        assert!(resolve_in(&fixture(), "claude-sonnet-4").is_none());
        assert_eq!(
            resolve_in(&fixture(), "gpt-5.5:low").and_then(|r| r.effort),
            Some("low".to_string())
        );
    }
}
