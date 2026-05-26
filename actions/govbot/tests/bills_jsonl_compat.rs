//! Back-compat contract test for `govbot logs` and the `bills.jsonl` shape.
//!
//! The chihacknight/govbot upgrade replaces the legacy `govbot logs` command
//! with `govbot source`. The most important downstream consumer is Frankie
//! Vegliante's `CHN-Bluesky-Govbot-Main` framework, which runs ~13 civic-
//! issue Bluesky bots, each driven by `govbot logs > bills.jsonl` in cron.
//! Its `scripts/post_to_bluesky.py` parser reads each line as a JSON object
//! and accesses a specific set of field paths. Breaking any of them silently
//! breaks every bot's next cron run.
//!
//! This test pins:
//!
//!   1. `govbot logs` runs (the back-compat alias survives).
//!   2. stdout is valid JSON-Lines.
//!   3. Every field path Frankie's parser accesses is present on at least
//!      one record:
//!        - `record.id`
//!        - `record.timestamp`
//!        - `record.bill.identifier`
//!        - `record.bill.title`
//!        - `record.bill.legislative_session`
//!        - `record.bill.abstracts[].abstract`  (when any abstract is present)
//!        - `record.bill.subject`               (when any subject is present)
//!        - `record.log.action.description`
//!        - `record.log.action.date`
//!        - `record.sources`                    (nested values contain `state:<xx>`)
//!   4. State detection works — `\bstate:([a-z]{2})\b` matches somewhere in
//!      the record on at least one line (Frankie's state-extraction regex).
//!   5. The dedup_key Frankie composes
//!      (`f"{state}|{identifier}|{action_date}|{action_desc[:40]}"`) is
//!      non-empty and stable across two consecutive invocations against the
//!      same mock corpus.
//!
//! Anyone who changes the shape `govbot source` emits (which `govbot logs`
//! aliases) gets a red test here before Frankie's bots see a broken cron.

use std::path::PathBuf;
use std::process::Command;

use regex::Regex;
use serde_json::Value;

/// Path to the freshly-built `govbot` binary. Mirrors the helper in
/// `run_repos_scope.rs` to keep this test binary self-contained.
fn govbot_binary() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let status = Command::new("cargo")
        .args(["build", "--bin", "govbot"])
        .current_dir(&manifest_dir)
        .status()
        .expect("cargo build should succeed");
    assert!(status.success(), "cargo build failed");
    manifest_dir.join("target").join("debug").join("govbot")
}

/// Path to the in-tree mock corpus — the same fixture `just govbot source`
/// uses for dev runs (`actions/govbot/mocks/.govbot`).
fn mocks_govbot_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("mocks")
        .join(".govbot")
}

/// Run `govbot logs --govbot-dir <mocks>` and return (stdout, stderr).
fn run_logs(govbot_dir: &std::path::Path) -> (String, String) {
    let bin = govbot_binary();
    let output = Command::new(&bin)
        .arg("logs")
        .arg("--govbot-dir")
        .arg(govbot_dir)
        .output()
        .expect("spawn govbot logs");
    assert!(
        output.status.success(),
        "govbot logs exited with {:?}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    (
        String::from_utf8(output.stdout).expect("stdout utf8"),
        String::from_utf8(output.stderr).expect("stderr utf8"),
    )
}

/// Parse JSON-Lines stdout into a `Vec<Value>`, skipping blank lines.
fn parse_jsonl(stdout: &str) -> Vec<Value> {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            serde_json::from_str::<Value>(l)
                .unwrap_or_else(|e| panic!("invalid JSON line: {e}: {l}"))
        })
        .collect()
}

/// Build the dedup key Frankie's `scripts/post_to_bluesky.py` composes:
/// `f"{state}|{identifier}|{action_date}|{action_desc[:40]}"`.
fn dedup_key(record: &Value) -> Option<String> {
    let state = state_from_record(record)?;
    let identifier = record
        .get("bill")
        .and_then(|b| b.get("identifier"))
        .and_then(|v| v.as_str())?;
    let action_date = record
        .get("log")
        .and_then(|l| l.get("action"))
        .and_then(|a| a.get("date"))
        .and_then(|v| v.as_str())?;
    let action_desc = record
        .get("log")
        .and_then(|l| l.get("action"))
        .and_then(|a| a.get("description"))
        .and_then(|v| v.as_str())?;
    let desc_head: String = action_desc.chars().take(40).collect();
    Some(format!("{state}|{identifier}|{action_date}|{desc_head}"))
}

/// Frankie's state-detection regex: `\bstate:([a-z]{2})\b` searched against
/// the JSON-encoded record (his code walks `record["sources"]` and any
/// nested strings; serializing the whole record is the same surface).
fn state_from_record(record: &Value) -> Option<String> {
    let re = Regex::new(r"\bstate:([a-z]{2})\b").expect("regex compiles");
    let flat = serde_json::to_string(record).ok()?;
    re.captures(&flat).map(|c| c[1].to_string())
}

/// (1) `govbot logs` survives the rename, (2) stdout is JSON-Lines, and (3)
/// every field path Frankie's parser touches is present on at least one
/// record. Coverage is "at least one record" — Frankie's parser walks the
/// stream and defends individual missing fields with `.get(...)` defaults;
/// the contract is that the SHAPE exists when the data does.
#[test]
fn govbot_logs_emits_every_field_frankie_reads() {
    let govbot_dir = mocks_govbot_dir();
    assert!(
        govbot_dir.exists(),
        "mock corpus missing at {}; run from actions/govbot/",
        govbot_dir.display()
    );

    let (stdout, stderr) = run_logs(&govbot_dir);
    let records = parse_jsonl(&stdout);
    assert!(
        !records.is_empty(),
        "govbot logs against the mock corpus emitted zero records — \
         the alias is wired up but Source produced no output. stderr:\n{stderr}"
    );

    // Top-level required-on-every-record fields.
    for (i, r) in records.iter().enumerate() {
        assert!(
            r.get("id").and_then(|v| v.as_str()).is_some(),
            "record[{i}] missing `id`: {r}"
        );
        assert!(
            r.get("timestamp").and_then(|v| v.as_str()).is_some(),
            "record[{i}] missing `timestamp`: {r}"
        );
        assert!(
            r.get("bill").and_then(|v| v.as_object()).is_some(),
            "record[{i}] missing `bill` object: {r}"
        );
        assert!(
            r.get("log").and_then(|v| v.as_object()).is_some(),
            "record[{i}] missing `log` object: {r}"
        );
        assert!(
            r.get("sources").is_some(),
            "record[{i}] missing `sources`: {r}"
        );
    }

    // Required bill subfields present on every record (mock corpus does
    // emit these for every bill).
    for (i, r) in records.iter().enumerate() {
        let bill = &r["bill"];
        assert!(
            bill.get("identifier").and_then(|v| v.as_str()).is_some(),
            "record[{i}].bill missing `identifier`: {bill}"
        );
        assert!(
            bill.get("title").and_then(|v| v.as_str()).is_some(),
            "record[{i}].bill missing `title`: {bill}"
        );
        assert!(
            bill.get("legislative_session")
                .and_then(|v| v.as_str())
                .is_some(),
            "record[{i}].bill missing `legislative_session`: {bill}"
        );
    }

    // Required log.action subfields on every record.
    for (i, r) in records.iter().enumerate() {
        let action = r["log"]
            .get("action")
            .expect(&format!("record[{i}].log.action missing"));
        assert!(
            action.get("description").and_then(|v| v.as_str()).is_some(),
            "record[{i}].log.action missing `description`: {action}"
        );
        assert!(
            action.get("date").and_then(|v| v.as_str()).is_some(),
            "record[{i}].log.action missing `date`: {action}"
        );
    }

    // `bill.abstracts[].abstract` — must be present on at least one
    // record when the underlying corpus has any abstract. The wy mock
    // has bills with `abstracts: [{abstract:..., note:"summary"}]`.
    let any_with_abstract = records.iter().any(|r| {
        r["bill"]
            .get("abstracts")
            .and_then(|a| a.as_array())
            .map(|arr| {
                arr.iter()
                    .any(|obj| obj.get("abstract").and_then(|v| v.as_str()).is_some())
            })
            .unwrap_or(false)
    });
    assert!(
        any_with_abstract,
        "no record exposed `bill.abstracts[].abstract` — Frankie's \
         abstract-text fallback path will never trigger. The wy mock \
         is known to carry abstracts; if this fails the source-side \
         abstracts projection has regressed."
    );

    // `bill.subject` — Frankie's parser does `record["bill"].get("subject", [])`,
    // so an absent key is tolerated (interpreted as no subjects). The hard
    // contract is the inverse: if `subject` IS present, it must be an array
    // of strings — anything else (object, scalar) would break his loop.
    // (Non-empty subjects are projected from the mocks when present; the
    // wy/gu mocks happen to ship `subject:[]` which Source omits by design,
    // pinned by `ocd_entry_to_doc_omits_subjects_when_subject_array_is_empty`
    // in main.rs.)
    for (i, r) in records.iter().enumerate() {
        if let Some(subj) = r["bill"].get("subject") {
            assert!(
                subj.is_array(),
                "record[{i}].bill.subject is not an array (type breaks \
                 Frankie's parser): {subj}"
            );
            for (j, s) in subj.as_array().unwrap().iter().enumerate() {
                assert!(
                    s.is_string(),
                    "record[{i}].bill.subject[{j}] is not a string: {s}"
                );
            }
        }
    }

    // `record.sources` nested strings must contain `state:<xx>` somewhere
    // — this is the regex anchor Frankie's parser uses to attribute a
    // record to a US state for the dedup_key and per-bot routing.
    let re = Regex::new(r"\bstate:([a-z]{2})\b").unwrap();
    let any_with_state = records.iter().any(|r| {
        r.get("sources")
            .map(|s| serde_json::to_string(s).unwrap_or_default())
            .map(|flat| re.is_match(&flat))
            .unwrap_or(false)
    });
    assert!(
        any_with_state,
        "no record's `sources` contained a `state:<xx>` substring; \
         Frankie's state-extraction regex will fail on every bot."
    );

    // Belt-and-suspenders: at least one record matches the regex when
    // serialized whole (sources or anywhere) — the broader form Frankie's
    // parser actually walks.
    let any_state_anywhere = records.iter().any(|r| state_from_record(r).is_some());
    assert!(
        any_state_anywhere,
        "no record yielded a state from the `\\bstate:([a-z]{{2}})\\b` \
         regex; Frankie's state attribution is dead."
    );

    // Deprecation warning lands on stderr (and ONLY stderr — stdout was
    // parsed as JSON-Lines above; any leakage would have failed the
    // `serde_json::from_str` line-by-line above).
    assert!(
        stderr.contains("`govbot logs` is deprecated"),
        "stderr did not carry the deprecation warning; got:\n{stderr}"
    );
}

/// (5) Dedup keys are non-empty and stable across two consecutive
/// invocations on the same mock data. Frankie's bots persist this key in
/// a posted-state ledger; instability would re-post every bill on every
/// cron run.
#[test]
fn dedup_key_is_nonempty_and_stable_across_runs() {
    let govbot_dir = mocks_govbot_dir();

    let (stdout_a, _) = run_logs(&govbot_dir);
    let (stdout_b, _) = run_logs(&govbot_dir);

    let records_a = parse_jsonl(&stdout_a);
    let records_b = parse_jsonl(&stdout_b);

    let keys_a: Vec<String> = records_a.iter().filter_map(dedup_key).collect();
    let keys_b: Vec<String> = records_b.iter().filter_map(dedup_key).collect();

    assert!(
        !keys_a.is_empty(),
        "first invocation produced zero non-empty dedup keys; Frankie's \
         ledger would be empty and every bill would re-post forever."
    );
    for k in &keys_a {
        let parts: Vec<&str> = k.split('|').collect();
        assert_eq!(
            parts.len(),
            4,
            "dedup_key not of shape state|identifier|date|desc[:40]: {k}"
        );
        for (i, p) in parts.iter().enumerate() {
            assert!(!p.is_empty(), "dedup_key part {i} is empty in: {k}");
        }
    }

    assert_eq!(
        keys_a, keys_b,
        "dedup keys diverged across two consecutive runs on the same mock \
         corpus — Frankie's bots would re-post every bill on every cron run."
    );
}
