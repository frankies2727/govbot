//! The `bluesky` publisher — posts matched bills to a Bluesky account.
//!
//! This is a **posting bot**, not a hosted AT-Protocol feed-generator service:
//! it posts to a normal Bluesky account via the XRPC API and runs to
//! completion (from cron/CI), so it needs no always-on server.
//!
//! Flow:
//!   1. Authenticate — `com.atproto.server.createSession` with the account
//!      handle + an **app password** read from the environment.
//!   2. Select records — keep those carrying a `select`ed tag whose calibrated
//!      `final_score` clears `min_score`.
//!   3. For each record not already in the ledger, render a post (<=300
//!      chars) and `com.atproto.repo.createRecord` an `app.bsky.feed.post`.
//!   4. Append the record's id to the posted-state ledger so re-runs never
//!      double-post.
//!
//! `--dry-run` renders the posts that *would* be sent and touches no network
//! and no ledger.
//!
//! Credentials are **environment-only** — never read from `govbot.yml`:
//!   - `BLUESKY_HANDLE`        — the account handle, e.g. `mybot.bsky.social`
//!   - `BLUESKY_APP_PASSWORD`  — an app password (Settings → App Passwords),
//!                               never the main account password
//!   - `BLUESKY_SERVICE`       — optional PDS base URL (default `https://bsky.social`)

use crate::publish::PublishJob;
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Bluesky's hard post-text limit (graphemes; we approximate with chars).
const POST_TEXT_LIMIT: usize = 300;

/// Default PDS service endpoint when `BLUESKY_SERVICE` is unset.
const DEFAULT_SERVICE: &str = "https://bsky.social";

/// The default post-text template. Kept deliberately simple — a future
/// `summarize` transform will improve framing.
const DEFAULT_TEMPLATE: &str = "{title}\n\n{tags} · {link}";

/// A post ready to be sent: the routing key (ledger id) plus rendered text.
#[derive(Debug)]
struct RenderedPost {
    /// The ledger key — a stable per-record id (the entry GUID).
    id: String,
    /// The post body, already truncated to the Bluesky limit.
    text: String,
}

/// Run the `bluesky` publisher against its result stream.
///
/// `dry_run` renders the would-be posts and touches no network and no ledger.
pub fn run_bluesky(job: &PublishJob, dry_run: bool) -> Result<()> {
    let p = job.publisher;
    let select = p.select.clone().unwrap_or_default();
    let min_score = p.resolved_min_score();

    // Resolve the ledger path (project-dir relative). Default: a per-publisher
    // file under `.govbot/`.
    let ledger_path = resolve_ledger_path(job);

    // Select records: a `select`ed tag must clear the calibrated threshold.
    let posts: Vec<RenderedPost> = job
        .entries
        .iter()
        .filter(|e| record_clears_threshold(e, &select, min_score))
        .map(|e| render_post(e, p.post_template.as_deref(), p.base_url.as_deref()))
        .collect();

    if posts.is_empty() {
        eprintln!(
            "Publisher '{}' (bluesky): no records cleared min_score {} for tags {} — nothing to post.",
            job.name,
            min_score,
            if select.is_empty() { "<all tagged>".to_string() } else { select.join(", ") }
        );
        return Ok(());
    }

    // Idempotency: drop records already in the posted-state ledger.
    let already_posted = read_ledger(&ledger_path)?;
    let pending: Vec<&RenderedPost> = posts
        .iter()
        .filter(|post| !already_posted.contains(&post.id))
        .collect();

    if dry_run {
        eprintln!(
            "Publisher '{}' (bluesky) --dry-run: {} record(s) cleared the threshold, \
             {} already posted, {} would be posted. No network, no ledger writes.",
            job.name,
            posts.len(),
            posts.len() - pending.len(),
            pending.len(),
        );
        for (i, post) in pending.iter().enumerate() {
            println!(
                "--- post {} of {} (id: {}) ---",
                i + 1,
                pending.len(),
                post.id
            );
            println!("{}", post.text);
            println!();
        }
        return Ok(());
    }

    if pending.is_empty() {
        eprintln!(
            "Publisher '{}' (bluesky): all {} matching record(s) already posted — nothing new.",
            job.name,
            posts.len()
        );
        return Ok(());
    }

    // Authenticate — credentials are environment-only. If they are absent,
    // skip the publisher with a WARN rather than failing the whole pipeline:
    // first-time activists running `govbot run` without Bluesky creds yet
    // should still get their RSS / HTML feeds rather than a red error.
    // Pair this with `govbot run --dry-run` to render-only without
    // requiring creds at all.
    if !creds_present() {
        eprintln!(
            "⚠️  Publisher '{}' (bluesky): BLUESKY_HANDLE / BLUESKY_APP_PASSWORD \
             not set — skipping. Set them (an app password from Bluesky \
             Settings → App Passwords) to go live; or use `govbot run \
             --dry-run` / `govbot publish --publisher {} --dry-run` to \
             render-only.",
            job.name, job.name
        );
        return Ok(());
    }
    let service = std::env::var("BLUESKY_SERVICE")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_SERVICE.to_string());
    let session = create_session(&service).context("Bluesky authentication failed")?;

    eprintln!(
        "Publisher '{}' (bluesky): authenticated as {} — posting {} record(s) to {}.",
        job.name,
        session.handle,
        pending.len(),
        service
    );

    // Post each pending record, appending to the ledger as we go so a
    // mid-run failure never re-posts what already succeeded.
    let mut posted = 0usize;
    for post in &pending {
        match create_post(&service, &session, &post.text) {
            Ok(uri) => {
                append_ledger(&ledger_path, &post.id)?;
                posted += 1;
                eprintln!("  ✓ posted {} -> {}", post.id, uri);
            }
            Err(e) => {
                // Fail loudly but stop — leave the rest for the next run
                // rather than hammering a failing endpoint.
                anyhow::bail!(
                    "Publisher '{}' (bluesky): posted {}/{} record(s); failed on {}: {}",
                    job.name,
                    posted,
                    pending.len(),
                    post.id,
                    e
                );
            }
        }
    }

    eprintln!(
        "✓ Publisher '{}' (bluesky): posted {} record(s); ledger at {}",
        job.name,
        posted,
        ledger_path.display()
    );
    Ok(())
}

// ============================================================
// Record selection + post rendering
// ============================================================

/// True when the record carries a `select`ed tag whose calibrated
/// `final_score` clears `min_score`. When `select` is empty, any tag counts.
///
/// The `tags` field is a map `tag_name -> ScoreBreakdown`; the calibrated
/// probability is `tags.<name>.final_score` (STREAM_PROTOCOL §5).
fn record_clears_threshold(entry: &Value, select: &[String], min_score: f64) -> bool {
    let tags = match entry.get("tags").and_then(|t| t.as_object()) {
        Some(t) if !t.is_empty() => t,
        _ => return false,
    };
    tags.iter().any(|(name, score)| {
        let selected = select.is_empty() || select.iter().any(|s| s == name);
        if !selected {
            return false;
        }
        score
            .get("final_score")
            .and_then(|v| v.as_f64())
            .map(|s| s >= min_score)
            .unwrap_or(false)
    })
}

/// Render a record into post text, applying the template and truncating to
/// the Bluesky character limit.
///
/// `base_url` — the publisher's `base_url` field — is the prefix prepended to
/// the bill's source-relative path when assembling `{link}`. Mirrors the
/// rss/html publishers' shape so a user can put a public URL into post text.
/// If `base_url` is `None`, the link falls back to the bill's
/// `bill.sources[0].url` (if present); otherwise `{link}` renders empty.
fn render_post(entry: &Value, template: Option<&str>, base_url: Option<&str>) -> RenderedPost {
    let id = crate::rss::extract_guid(entry);
    let template = template.unwrap_or(DEFAULT_TEMPLATE);

    let title = bill_title(entry);
    let tags = entry
        .get("tags")
        .and_then(|t| t.as_object())
        .map(|m| m.keys().cloned().collect::<Vec<_>>().join(", "))
        .unwrap_or_default();
    let link = crate::rss::extract_link(entry, base_url).unwrap_or_default();
    let identifier = entry
        .get("bill")
        .and_then(|b| b.get("identifier"))
        .and_then(|v| v.as_str())
        .or_else(|| entry.get("id").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();
    let session = entry
        .get("bill")
        .and_then(|b| b.get("legislative_session"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let score = top_score(entry)
        .map(|s| format!("{:.2}", s))
        .unwrap_or_default();

    let text = template
        .replace("{title}", &title)
        .replace("{tags}", &tags)
        .replace("{link}", &link)
        .replace("{identifier}", &identifier)
        .replace("{session}", &session)
        .replace("{score}", &score);

    RenderedPost {
        id,
        text: truncate_post(&text),
    }
}

/// The highest calibrated `final_score` across a record's tags.
fn top_score(entry: &Value) -> Option<f64> {
    entry
        .get("tags")
        .and_then(|t| t.as_object())
        .and_then(|tags| {
            tags.values()
                .filter_map(|s| s.get("final_score").and_then(|v| v.as_f64()))
                .fold(None, |acc, s| Some(acc.map_or(s, |a: f64| a.max(s))))
        })
}

/// Best-effort bill title — the bill's `title`, else its identifier, else a
/// generic fallback.
fn bill_title(entry: &Value) -> String {
    if let Some(t) = entry
        .get("bill")
        .and_then(|b| b.get("title"))
        .and_then(|v| v.as_str())
    {
        let t = t.trim();
        if !t.is_empty() {
            return t.to_string();
        }
    }
    if let Some(id) = entry
        .get("bill")
        .and_then(|b| b.get("identifier"))
        .and_then(|v| v.as_str())
        .or_else(|| entry.get("id").and_then(|v| v.as_str()))
    {
        if !id.is_empty() {
            return id.to_string();
        }
    }
    "Legislative update".to_string()
}

/// Truncate post text to the Bluesky limit, appending an ellipsis when cut.
fn truncate_post(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= POST_TEXT_LIMIT {
        return trimmed.to_string();
    }
    let mut out: String = trimmed.chars().take(POST_TEXT_LIMIT - 1).collect();
    // Avoid cutting mid-word where reasonable.
    if let Some(idx) = out.rfind(char::is_whitespace) {
        if idx > POST_TEXT_LIMIT / 2 {
            out.truncate(idx);
        }
    }
    format!("{}…", out.trim_end())
}

// ============================================================
// Posted-state ledger (idempotency)
// ============================================================

/// Resolve the ledger file path: the publisher's `ledger` field if set,
/// else `<project>/.govbot/bluesky-<name>.ledger`. Relative paths resolve
/// against the project directory (where `govbot.yml` lives).
fn resolve_ledger_path(job: &PublishJob) -> PathBuf {
    match &job.publisher.ledger {
        Some(p) => {
            let p = PathBuf::from(p);
            if p.is_absolute() {
                p
            } else {
                job.project_dir.join(p)
            }
        }
        None => job
            .project_dir
            .join(".govbot")
            .join(format!("bluesky-{}.ledger", job.name)),
    }
}

/// Read the set of already-posted record ids from the ledger. A missing
/// ledger is an empty set (first run). The ledger is append-only,
/// newline-delimited, one record id per line.
fn read_ledger(path: &Path) -> Result<HashSet<String>> {
    if !path.exists() {
        return Ok(HashSet::new());
    }
    let contents = fs::read_to_string(path)
        .with_context(|| format!("Failed to read posted-state ledger: {}", path.display()))?;
    Ok(contents
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(|l| l.to_string())
        .collect())
}

/// Append a posted record id to the ledger, creating it (and its parent
/// directory) if needed.
fn append_ledger(path: &Path, id: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create ledger directory: {}", parent.display()))?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to open posted-state ledger: {}", path.display()))?;
    writeln!(file, "{}", id)
        .with_context(|| format!("Failed to append to ledger: {}", path.display()))?;
    Ok(())
}

// ============================================================
// AT Protocol XRPC
// ============================================================

/// An authenticated Bluesky session.
struct Session {
    /// The bearer access token (`accessJwt`).
    access_jwt: String,
    /// The repo DID — the record owner for `createRecord`.
    did: String,
    /// The resolved account handle (for logging).
    handle: String,
}

/// Authenticate via `com.atproto.server.createSession`.
///
/// Reads `BLUESKY_HANDLE` + `BLUESKY_APP_PASSWORD` from the environment;
/// these are required and never sourced from `govbot.yml`.
fn create_session(service: &str) -> Result<Session> {
    let handle = require_env("BLUESKY_HANDLE")?;
    let password = require_env("BLUESKY_APP_PASSWORD")?;

    let url = format!(
        "{}/xrpc/com.atproto.server.createSession",
        service.trim_end_matches('/')
    );
    // `http_status_as_error(false)` keeps a non-2xx response an `Ok` so we can
    // read its body for an actionable error; only transport errors are `Err`.
    let response = ureq::post(&url)
        .config()
        .http_status_as_error(false)
        .build()
        .header("Content-Type", "application/json")
        .send_json(json!({ "identifier": handle, "password": password }))
        .context("createSession request failed")?;

    let status = response.status();
    let mut resp_body = response.into_body();
    if !status.is_success() {
        let detail = resp_body
            .read_to_string()
            .unwrap_or_else(|_| "<no body>".to_string());
        anyhow::bail!(
            "createSession returned HTTP {} — check BLUESKY_HANDLE / \
             BLUESKY_APP_PASSWORD (use an app password, not the main \
             password). Response: {}",
            status.as_u16(),
            detail
        );
    }
    let body: Value = resp_body
        .read_json()
        .context("Failed to parse createSession response")?;

    let access_jwt = body
        .get("accessJwt")
        .and_then(|v| v.as_str())
        .context("createSession response missing accessJwt")?
        .to_string();
    let did = body
        .get("did")
        .and_then(|v| v.as_str())
        .context("createSession response missing did")?
        .to_string();
    let handle = body
        .get("handle")
        .and_then(|v| v.as_str())
        .unwrap_or(&handle)
        .to_string();

    Ok(Session {
        access_jwt,
        did,
        handle,
    })
}

/// Post one `app.bsky.feed.post` record via `com.atproto.repo.createRecord`.
/// Returns the AT URI of the created record.
fn create_post(service: &str, session: &Session, text: &str) -> Result<String> {
    let url = format!(
        "{}/xrpc/com.atproto.repo.createRecord",
        service.trim_end_matches('/')
    );
    // RFC-3339 UTC timestamp, as the AT Protocol expects for `createdAt`.
    let created_at = chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();

    let response = ureq::post(&url)
        .config()
        .http_status_as_error(false)
        .build()
        .header("Authorization", &format!("Bearer {}", session.access_jwt))
        .header("Content-Type", "application/json")
        .send_json(json!({
            "repo": session.did,
            "collection": "app.bsky.feed.post",
            "record": {
                "$type": "app.bsky.feed.post",
                "text": text,
                "createdAt": created_at,
            }
        }))
        .context("createRecord request failed")?;

    let status = response.status();
    let mut resp_body = response.into_body();
    if !status.is_success() {
        let detail = resp_body
            .read_to_string()
            .unwrap_or_else(|_| "<no body>".to_string());
        anyhow::bail!("createRecord returned HTTP {}: {}", status.as_u16(), detail);
    }
    let body: Value = resp_body
        .read_json()
        .context("Failed to parse createRecord response")?;

    Ok(body
        .get("uri")
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown>")
        .to_string())
}

/// True when both required Bluesky credential env vars are set and non-empty.
/// Used to decide whether the publisher should skip-with-WARN (missing creds)
/// or attempt the live authentication flow.
fn creds_present() -> bool {
    env_nonempty("BLUESKY_HANDLE") && env_nonempty("BLUESKY_APP_PASSWORD")
}

/// True when `key` is set to a non-empty (and non-whitespace) value.
fn env_nonempty(key: &str) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
}

/// Read a required environment variable, with an actionable error message.
fn require_env(key: &str) -> Result<String> {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .with_context(|| {
            format!(
                "the `bluesky` publisher needs the {key} environment variable. \
                 Set BLUESKY_HANDLE and BLUESKY_APP_PASSWORD (an app password \
                 from Bluesky Settings → App Passwords). Never put credentials \
                 in govbot.yml."
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn truncate_respects_limit() {
        let long = "word ".repeat(100);
        let out = truncate_post(&long);
        assert!(out.chars().count() <= POST_TEXT_LIMIT);
        assert!(out.ends_with('…'));
    }

    #[test]
    fn truncate_leaves_short_text_alone() {
        assert_eq!(truncate_post("  hello  "), "hello");
    }

    #[test]
    fn threshold_selects_on_calibrated_score() {
        let entry = json!({
            "tags": { "clean_energy": { "final_score": 0.8 } }
        });
        assert!(record_clears_threshold(&entry, &[], 0.6));
        assert!(record_clears_threshold(
            &entry,
            &["clean_energy".to_string()],
            0.6
        ));
        assert!(!record_clears_threshold(&entry, &[], 0.9));
        assert!(!record_clears_threshold(
            &entry,
            &["fossil_fuels".to_string()],
            0.6
        ));
    }

    #[test]
    fn threshold_rejects_untagged() {
        assert!(!record_clears_threshold(&json!({}), &[], 0.0));
        assert!(!record_clears_threshold(&json!({ "tags": {} }), &[], 0.0));
    }

    /// When BLUESKY_HANDLE / BLUESKY_APP_PASSWORD are absent, `creds_present`
    /// reports `false` — the signal that lets `run_bluesky` skip with a WARN
    /// instead of failing the whole pipeline. With both set non-empty,
    /// `true`.
    ///
    /// This test mutates process env; `cargo test` runs threads in parallel by
    /// default, so it locks a mutex around the env touch.
    #[test]
    fn creds_present_reflects_env() {
        // Serialise env mutation across the env-touching tests so parallel
        // test threads can't see each other's writes mid-check.
        use std::sync::Mutex;
        static ENV_LOCK: Mutex<()> = Mutex::new(());
        let _g = ENV_LOCK.lock().unwrap();

        // Snapshot original values to restore at the end.
        let prev_h = std::env::var("BLUESKY_HANDLE").ok();
        let prev_p = std::env::var("BLUESKY_APP_PASSWORD").ok();

        std::env::remove_var("BLUESKY_HANDLE");
        std::env::remove_var("BLUESKY_APP_PASSWORD");
        assert!(!creds_present());

        std::env::set_var("BLUESKY_HANDLE", "x.bsky.social");
        assert!(!creds_present()); // password still missing

        std::env::set_var("BLUESKY_APP_PASSWORD", "abcd-efgh-ijkl-mnop");
        assert!(creds_present());

        std::env::set_var("BLUESKY_HANDLE", "   "); // whitespace-only
        assert!(!creds_present());

        // Restore.
        match prev_h {
            Some(v) => std::env::set_var("BLUESKY_HANDLE", v),
            None => std::env::remove_var("BLUESKY_HANDLE"),
        }
        match prev_p {
            Some(v) => std::env::set_var("BLUESKY_APP_PASSWORD", v),
            None => std::env::remove_var("BLUESKY_APP_PASSWORD"),
        }
    }

    #[test]
    fn render_substitutes_template_placeholders() {
        let entry = json!({
            "id": "wy-legislation/.../HB0001",
            "bill": { "title": "Renewable energy storage act", "identifier": "HB 1" },
            "tags": { "clean_energy": { "final_score": 0.92 } }
        });
        let post = render_post(&entry, Some("{title} [{identifier}] {tags} {score}"), None);
        assert!(post.text.contains("Renewable energy storage act"));
        assert!(post.text.contains("[HB 1]"));
        assert!(post.text.contains("clean_energy"));
        assert!(post.text.contains("0.92"));
    }

    /// `{link}` renders the publisher's `base_url` joined to the bill's
    /// source-log path — same shape as the rss/html publishers. Before the
    /// fix, bluesky passed `None` and `{link}` rendered empty.
    #[test]
    fn render_link_uses_publisher_base_url() {
        let entry = json!({
            "id": "wy-legislation/.../HB0001",
            "bill": { "title": "Wind energy permitting act", "identifier": "HB 1" },
            "sources": { "bill": "wy-legislation/.../HB0001/metadata.json" },
            "tags": { "clean_energy": { "final_score": 0.91 } }
        });
        let post = render_post(
            &entry,
            Some("{title} {link}"),
            Some("https://example.org/climate-tracker"),
        );
        assert!(
            post.text.contains(
                "https://example.org/climate-tracker/wy-legislation/.../HB0001/metadata.json"
            ),
            "expected base_url to be prepended to source path; got: {}",
            post.text
        );
    }

    /// Without a configured `base_url`, `{link}` falls back to the bill's
    /// `bill.sources[0].url` (when present) — preserves the historical
    /// shape and gives manifest authors a sensible default before they pick
    /// a base_url.
    #[test]
    fn render_link_falls_back_to_bill_source_url() {
        let entry = json!({
            "id": "wy-legislation/.../HB0001",
            "bill": {
                "title": "Solar tax-credit act",
                "identifier": "HB 1",
                "sources": [{ "url": "https://wyoleg.gov/2025/Bills/HB0001" }]
            },
            "tags": { "clean_energy": { "final_score": 0.9 } }
        });
        let post = render_post(&entry, Some("{title} -> {link}"), None);
        assert!(
            post.text.contains("https://wyoleg.gov/2025/Bills/HB0001"),
            "expected bill.sources[0].url to render as {{link}}; got: {}",
            post.text
        );
    }
}
