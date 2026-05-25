//! Regression test for the `datasets:[wy]` scope leak.
//!
//! `govbot::pipeline::run_transform_dag` spawns `govbot source --select docs`
//! as the head of the classify pipeline. Pre-fix it never passed `--repos`,
//! so the manifest's `datasets:` was silently ignored at the source step: a
//! manifest declaring `datasets: [wy]` in a project whose `.govbot/repos/`
//! held 52 datasets (left over from an earlier `[all]` pull) classified
//! ~4900 records across every state instead of ~100 Wyoming records.
//!
//! The fix translates `manifest.datasets` to a `--repos <list>` argv that
//! gets appended to the source spawn. This test pins the two invariants the
//! fix relies on:
//!
//!  1. `govbot source --select docs --repos <dataset> ...` against a
//!     multi-dataset cache emits records only from the named dataset(s).
//!     This is the source-side scoping the pipeline relies on — if it ever
//!     regresses, the pipeline's `--repos` plumbing is moot.
//!  2. Omitting `--repos` walks every linked dataset — the "every dataset"
//!     sentinel `source_repos_from_manifest` produces for a `[all]`
//!     manifest, so the pipeline can keep treating absence as "all".
//!
//! Together with the `source_repos_from_manifest` unit test in
//! `pipeline.rs` (which pins the manifest→argv translation), these
//! invariants regression-test the full fix path.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

/// Path to the freshly-built `govbot` binary. Mirrors the helper in
/// `cli_example_snaps.rs` but kept local so the two integration test
/// binaries stay independent.
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

/// Build a throwaway `.govbot/repos/` tree with two datasets (`wy`, `gu`),
/// each holding one bill with one log file. Returns the absolute path to the
/// `.govbot` root (the value to pass as `--govbot-dir`).
///
/// We materialise the on-disk corpus by hand rather than re-using
/// `actions/govbot/mocks/` because the shipped mock's `gu-legislation/` has
/// metadata but no logs — `govbot source` emits per-log entries, so a
/// "did the filter scope to wy" assertion is vacuous if `gu` has no logs to
/// scope away.
fn build_two_dataset_corpus(tmp: &std::path::Path) -> PathBuf {
    let repos = tmp.join(".govbot").join("repos");

    for (dataset, state, bill_id) in [
        ("wy-legislation", "wy", "HB0001"),
        ("gu-legislation", "gu", "B1-38"),
    ] {
        // Layout the walker expects: `country:<c>/state:<s>/sessions/<id>/bills/<bill>/logs/<ts>_<action>.json`.
        let session = if state == "wy" { "2025" } else { "38th" };
        let bill_dir = repos
            .join(dataset)
            .join("country:us")
            .join(format!("state:{}", state))
            .join("sessions")
            .join(session)
            .join("bills")
            .join(bill_id);
        let logs_dir = bill_dir.join("logs");
        fs::create_dir_all(&logs_dir).expect("create logs dir");

        // A minimal metadata.json — `source --select docs` joins it for the
        // doc text. The timestamp on the log filename is what the source
        // walker sorts by; the suffix names the action.
        fs::write(
            bill_dir.join("metadata.json"),
            serde_json::json!({
                "title": format!("Test bill {}", bill_id),
                "identifier": bill_id,
                "subjects": ["test"],
                "abstracts": [{"abstract": format!("Body of {}", bill_id)}],
            })
            .to_string(),
        )
        .expect("write metadata.json");

        // A "passage" log — substantive under `--filter default`, so the
        // record survives the default filter and shows up in `--select docs`.
        fs::write(
            logs_dir.join("20250129T022703Z_passage.json"),
            serde_json::json!({
                "action": "passage",
                "bill_id": bill_id,
                "date": "2025-01-29",
            })
            .to_string(),
        )
        .expect("write log");
    }

    tmp.join(".govbot")
}

/// Collect `govbot source --select docs` stdout against the throwaway
/// corpus, parsed into one JSON value per non-empty line. The `--filter
/// none` keeps every log entry — we want the count to depend only on
/// `--repos` scoping, not on the per-dataset action filters.
fn source_docs(govbot_dir: &std::path::Path, repos: &[&str]) -> Vec<serde_json::Value> {
    let bin = govbot_binary();
    let mut cmd = Command::new(&bin);
    cmd.arg("source")
        .arg("--select")
        .arg("docs")
        .arg("--filter")
        .arg("none")
        .arg("--govbot-dir")
        .arg(govbot_dir);
    if !repos.is_empty() {
        cmd.arg("--repos");
        for r in repos {
            cmd.arg(r);
        }
    }
    let output = cmd.output().expect("spawn govbot source");
    assert!(
        output.status.success(),
        "govbot source exited with {:?}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .collect()
}

/// Pin invariant (1): `--repos wy` against a `wy+gu` corpus emits only `wy`
/// records. This is the source-side guarantee the pipeline relies on.
#[test]
fn source_with_repos_scopes_to_named_dataset() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let govbot_dir = build_two_dataset_corpus(tmp.path());

    let wy_only = source_docs(&govbot_dir, &["wy"]);
    assert!(
        !wy_only.is_empty(),
        "wy corpus should emit at least one record"
    );
    for record in &wy_only {
        let id = record
            .get("id")
            .and_then(|v| v.as_str())
            .expect("doc record must have a string `id`");
        assert!(
            id.starts_with("wy-legislation/"),
            "--repos wy leaked a non-wy record: {}",
            id
        );
    }
}

/// Pin invariant (2): omitting `--repos` walks every linked dataset. This is
/// what `source_repos_from_manifest(&["all"])` returns (empty list → flag
/// omitted), and the pipeline relies on that translation matching source's
/// own "all" sentinel.
#[test]
fn source_without_repos_walks_every_dataset() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let govbot_dir = build_two_dataset_corpus(tmp.path());

    let all = source_docs(&govbot_dir, &[]);
    let datasets: std::collections::BTreeSet<String> = all
        .iter()
        .filter_map(|r| r.get("id").and_then(|v| v.as_str()))
        .filter_map(|id| id.split('/').next().map(str::to_string))
        .collect();
    assert!(
        datasets.contains("wy-legislation") && datasets.contains("gu-legislation"),
        "no-`--repos` walk should hit both datasets, got: {:?}",
        datasets
    );
}
