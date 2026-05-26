//! Integration test for `govbot init --from-frankie-config <path>` — the
//! migration tool that scaffolds a govbot+fastclass project from an existing
//! Frankie-style topic config.
//!
//! Asserts the produced skeleton:
//!  - Has a valid govbot manifest (`govbot.yml` parses).
//!  - Has a classifier bundle with exactly one tag named after the Frankie
//!    `name`, whose `include_keywords` equal the fixture's keyword list.
//!  - Has a `fusion.yml` declaring the portable `models:` block.
//!  - Refuses to overwrite an existing project (idempotency guard).
//!
//! Mirrors the style of `run_repos_scope.rs` — builds the binary, runs it as
//! a subprocess, and inspects the on-disk output.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

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

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("frankie_transportation_config.yml")
}

#[test]
fn from_frankie_config_scaffolds_a_valid_govbot_project() {
    let bin = govbot_binary();
    let fixture = fixture_path();
    let tmp = tempfile::tempdir().expect("tempdir");
    let into = tmp.path().join("scratch-transport");

    // --- Run: govbot init --from-frankie-config <fixture> --into <tmpdir> ---
    let output = Command::new(&bin)
        .args([
            "init",
            "--from-frankie-config",
            fixture.to_str().unwrap(),
            "--into",
            into.to_str().unwrap(),
        ])
        .output()
        .expect("govbot init should execute");
    assert!(
        output.status.success(),
        "govbot init failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // --- 1. govbot.yml parses as a valid manifest. ---
    let manifest_path = into.join("govbot.yml");
    assert!(
        manifest_path.exists(),
        "expected scaffolded govbot.yml at {}",
        manifest_path.display()
    );
    let manifest =
        govbot::config::Manifest::load(&manifest_path).expect("scaffolded govbot.yml should parse");
    assert_eq!(
        manifest.datasets,
        vec!["all".to_string()],
        "scaffolded manifest should default to datasets: [all]"
    );
    assert!(
        manifest.transforms.contains_key("classify"),
        "manifest should declare a `classify` transform"
    );
    assert!(
        manifest.publish.contains_key("bluesky"),
        "manifest should declare a `bluesky` publisher"
    );
    // bluesky select carries the topic name (single tag = the topic).
    let bluesky = manifest.publish.get("bluesky").unwrap();
    assert_eq!(
        bluesky.select.as_deref().map(|s| s.to_vec()),
        Some(vec!["transportation".to_string()])
    );

    // --- 2. classifier.yml has one tag named after `name`; keywords match. ---
    let classifier_yml_path = into.join("classifier").join("classifier.yml");
    assert!(classifier_yml_path.exists(), "classifier.yml should exist");
    let raw = fs::read_to_string(&classifier_yml_path).expect("read classifier.yml");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&raw).expect("classifier.yml is YAML");
    let tags = parsed
        .get("tags")
        .and_then(|v| v.as_mapping())
        .expect("classifier.yml should carry a `tags:` mapping");
    assert_eq!(
        tags.len(),
        1,
        "scaffolded classifier should carry exactly one tag (the Frankie topic name)"
    );
    let tag = tags
        .get(serde_yaml::Value::String("transportation".to_string()))
        .expect("the single tag should be named after the Frankie `name`");
    let include_keywords: Vec<String> = tag
        .get("include_keywords")
        .and_then(|v| v.as_sequence())
        .expect("tag should carry include_keywords")
        .iter()
        .map(|v| v.as_str().expect("keyword is a string").to_string())
        .collect();
    let expected_keywords = vec![
        "public transit",
        "bus rapid transit",
        "light rail",
        "high-speed rail",
        "bike lane",
        "pedestrian safety",
        "vision zero",
        "electric vehicle",
        "EV charging",
        "road infrastructure",
    ];
    assert_eq!(
        include_keywords, expected_keywords,
        "include_keywords should mirror the Frankie keyword list verbatim"
    );

    // --- 3. fusion.yml declares the portable models: block. ---
    let fusion_path = into.join("classifier").join("fusion.yml");
    assert!(fusion_path.exists(), "fusion.yml should exist");
    let fusion_raw = fs::read_to_string(&fusion_path).expect("read fusion.yml");
    let fusion: serde_yaml::Value =
        serde_yaml::from_str(&fusion_raw).expect("fusion.yml should parse");
    let models = fusion
        .get("models")
        .and_then(|v| v.as_mapping())
        .expect("fusion.yml should declare a `models:` block");
    assert!(
        models.contains_key(serde_yaml::Value::String("encoder".to_string())),
        "models: should declare an encoder"
    );
    assert!(
        models.contains_key(serde_yaml::Value::String("reranker".to_string())),
        "models: should declare a reranker"
    );

    // --- supporting files exist ---
    assert!(into
        .join("classifier")
        .join("eval")
        .join("constitution.yml")
        .exists());
    assert!(into
        .join("classifier")
        .join("eval")
        .join("rolling.yml")
        .exists());
    assert!(into.join("classifier").join("proposals").exists());
    assert!(into.join("summarizer").join("prompt.md").exists());
    assert!(into.join("README.md").exists());
    assert!(into.join(".gitignore").exists());

    // --- 4. Re-running into the same dir refuses to overwrite. ---
    let rerun = Command::new(&bin)
        .args([
            "init",
            "--from-frankie-config",
            fixture.to_str().unwrap(),
            "--into",
            into.to_str().unwrap(),
        ])
        .output()
        .expect("re-run should execute");
    assert!(
        !rerun.status.success(),
        "re-running --from-frankie-config into an existing project must fail"
    );
    let stderr = String::from_utf8_lossy(&rerun.stderr);
    assert!(
        stderr.contains("already exists") || stderr.contains("refusing"),
        "stderr should explain the overwrite guard; got: {}",
        stderr
    );
}
