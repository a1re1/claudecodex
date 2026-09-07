# claudecodex

An Anthropic Messages-API-compatible HTTP proxy, written in Rust, that lets
[Claude Code](https://docs.anthropic.com/en/docs/claude-code) drive OpenAI /
Codex models through your existing ChatGPT (Codex) subscription.

Claude Code talks Anthropic `/v1/messages`; the proxy translates each request
into an OpenAI Responses API call against the Codex backend
(`chatgpt.com/backend-api/codex`), using the OAuth tokens the `codex` CLI
already stores in `~/.codex/auth.json`, and streams the answer back as
Anthropic SSE. No Codex app-server, no OpenAI API key.

## Prerequisites

- Rust toolchain (`cargo`).
- The Codex CLI, logged in with a ChatGPT account: `codex login`. The proxy
  reads (and, when the access token expires, refreshes and rewrites) only
  `auth.json` under the Codex home directory.

## Run

```sh
cargo run --release -- --auth-token secret
```

Then, in another terminal:

```sh
ANTHROPIC_BASE_URL="http://127.0.0.1:8787" \
ANTHROPIC_AUTH_TOKEN="secret" \
ANTHROPIC_API_KEY="" \
CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY=1 \
ANTHROPIC_DEFAULT_OPUS_MODEL=claude-gpt-5.5 \
ANTHROPIC_DEFAULT_SONNET_MODEL=claude-gpt-5.5 \
ANTHROPIC_DEFAULT_HAIKU_MODEL=claude-gpt-5.4-mini:low \
CLAUDE_CODE_DISABLE_ADAPTIVE_THINKING=1 \
claude
```

Claude Code fetches `GET /v1/models` at startup (gateway model discovery),
caches the list in `~/.claude/cache/gateway-models.json`, and lets you pick
any of the proxied models with `/model`.

## Model ids

Claude Code only keeps gateway models whose id contains `claude`, so every
Codex model is exposed as `claude-<codex slug>`; the proxy strips the prefix
before calling upstream (bare slugs are accepted too). An optional
`:<effort>` suffix pins the reasoning effort, e.g. `claude-gpt-5.5:low`,
`claude-gpt-6-astra:xhigh`. Efforts are clamped to what the model supports.

Effort precedence: id suffix, then the request's `thinking.budget_tokens`
(mapped to a level), then `--default-reasoning-effort`, then the model's
default. The live list of slugs and supported efforts comes from the Codex
`/models` endpoint and is cached for ten minutes.

Reasoning summaries come back as Anthropic `thinking` blocks. The encrypted
reasoning payload is carried in the block's `signature` so it round-trips on
the next turn.

## Endpoints

| Method | Path | Notes |
| --- | --- | --- |
| `POST` | `/v1/messages` | Streaming (SSE) and non-streaming. |
| `POST` | `/v1/messages/count_tokens` | Rough estimate (bytes / 4); the Codex backend has no counting endpoint. |
| `GET` | `/v1/models` | Anthropic-shaped list of `claude-<slug>[:effort]` ids. |
| `GET`/`HEAD` | `/api/hello` | Health check, unauthenticated. |

Authentication: `Authorization: Bearer <token>` or `x-api-key: <token>`,
matched against `--auth-token`. If no token is configured the proxy is open
(it binds to loopback by default). Upstream rate-limit headers
(`x-codex-primary-used-percent`, `x-codex-primary-reset-after-seconds`,
`x-codex-plan-type`) are forwarded on every response.

## Configuration

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--port` | | `8787` | Listen port. |
| `--bind` | | `127.0.0.1` | Listen address. |
| `--auth-token` | `CLAUDECODEX_AUTH_TOKEN` | none | Token clients must present. |
| `--codex-home` | `CODEX_HOME` | `~/.codex` | Directory containing `auth.json`. |
| `--upstream-base` | `CLAUDECODEX_UPSTREAM` | `https://chatgpt.com/backend-api/codex` | Codex backend base URL. |
| `--default-reasoning-effort` | | `medium` | Effort when neither the id nor the request specifies one. |
| `--verbose` | | off | Debug logging (`RUST_LOG` also honoured). |

## Limitations

- Anthropic server tools (web search, computer use, etc.) are not supported;
  only client-side function tools are forwarded.
- PDF / document content blocks are dropped; images are forwarded.
- `max_tokens` is accepted but not enforced upstream (the Codex backend
  rejects `max_output_tokens`).
- `count_tokens` is an estimate.
- Prompt caching stats are reported from upstream `cached_tokens`; there is no
  explicit cache control.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```
