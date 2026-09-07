mod anthropic;
mod auth;
mod models;
mod server;
mod translate;
mod upstream;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tracing_subscriber::EnvFilter;

/// Anthropic Messages API proxy for Codex (ChatGPT login) models.
#[derive(Parser, Debug)]
#[command(name = "claudecodex", version)]
struct Cli {
    /// TCP port to listen on.
    #[arg(long, default_value_t = 8787)]
    port: u16,
    /// Address to bind.
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,
    /// Bearer token Claude Code must present (its ANTHROPIC_AUTH_TOKEN). Unset disables the check.
    #[arg(long, env = "CLAUDECODEX_AUTH_TOKEN")]
    auth_token: Option<String>,
    /// Codex home directory containing auth.json (default: ~/.codex).
    #[arg(long, env = "CODEX_HOME")]
    codex_home: Option<PathBuf>,
    /// Upstream Codex backend base URL.
    #[arg(long, env = "CLAUDECODEX_UPSTREAM", default_value = upstream::DEFAULT_BASE)]
    upstream_base: String,
    /// Reasoning effort used when the model id carries no `:<effort>` suffix.
    #[arg(long, default_value = "medium")]
    default_reasoning_effort: String,
    /// Verbose logging (same as RUST_LOG=claudecodex=debug).
    #[arg(short, long)]
    verbose: bool,
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let default_filter = if cli.verbose {
        "claudecodex=debug,info"
    } else {
        "info"
    };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let codex_home = cli.codex_home.unwrap_or_else(|| home_dir().join(".codex"));
    let auth = Arc::new(auth::AuthStore::load(&codex_home).await?);
    let upstream = Arc::new(upstream::Upstream::new(cli.upstream_base, auth)?);
    let catalog = Arc::new(models::Catalog::new(upstream.clone()));
    let state = server::AppState {
        auth_token: cli.auth_token.filter(|t| !t.is_empty()),
        upstream,
        catalog,
        default_effort: cli.default_reasoning_effort,
    };

    let addr = format!("{}:{}", cli.bind, cli.port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    tracing::info!(
        %addr,
        auth_required = state.auth_token.is_some(),
        codex_home = %codex_home.display(),
        "claudecodex listening"
    );
    axum::serve(listener, server::router(state)).await?;
    Ok(())
}
