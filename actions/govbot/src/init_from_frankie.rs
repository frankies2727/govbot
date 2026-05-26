//! `govbot init --from-frankie-config` — migration tool that scaffolds a
//! govbot+fastclass project from a Frankie-style per-topic config.
//!
//! Frankie is the original CHN-Bluesky-Govbot framework. Each topic
//! (`transportation`, `immigration`, `housing`, …) lives in a
//! `topics/<name>/config.yml` carrying a name, display name, default emoji,
//! a flat keyword list covering subdomains, a keyword→emoji map, a digest
//! title, and a topic focus. This module reads one such file and emits a
//! govbot+fastclass project skeleton — a `govbot.yml` manifest plus a
//! fastclass classifier bundle plus the supporting stubs — so an existing
//! Frankie topic maintainer can migrate to the new stack without rebuilding
//! the keyword list from scratch.
//!
//! ### Field-to-field mapping
//!
//! | Frankie field    | Scaffolded output                                      |
//! |------------------|--------------------------------------------------------|
//! | `name`           | classifier tag name + bluesky `select: [<name>]`       |
//! | `display_name`   | tag description framing + README header                |
//! | `default_emoji`  | README header + summarizer prompt voice                |
//! | `keywords`       | `classifier/classifier.yml: tags.<name>.include_keywords` |
//! | `emoji_map`      | classifier.yml comment listing the keyword→emoji map   |
//! | `digest_title`   | `publish.feed.title` + `publish.site.title`            |
//! | `topic`          | tag description + summarizer prompt subject            |
//!
//! No network calls — purely a local-file transformation.
//!
//! ### Atomicity
//!
//! Refuses to overwrite if `<into>/govbot.yml` already exists. Otherwise
//! scaffolds everything before reporting success: a failure mid-write leaves
//! a partial tree (the user can `rm -rf <into>` and retry), but the
//! pre-flight check is the primary guard against clobbering an existing
//! project.

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// The Frankie per-topic config shape. Permissive: extra fields are absorbed
/// into `extra` so a Frankie config that carries fields we don't use yet
/// (timezone, schedule, jurisdictions, …) still parses cleanly.
#[derive(Debug, Deserialize)]
pub struct FrankieTopicConfig {
    /// The machine-readable topic name (e.g. `"transportation"`). Becomes the
    /// classifier's single tag name and the bluesky publisher's `select` entry.
    pub name: String,

    /// Optional human-readable display name (e.g. `"Transportation"`).
    /// Defaults to a title-cased `name` if absent.
    pub display_name: Option<String>,

    /// Default emoji for the topic (e.g. `"🚗"`).
    pub default_emoji: Option<String>,

    /// The flat keyword list covering the topic's subdomains. Becomes the
    /// classifier tag's `include_keywords`.
    #[serde(default)]
    pub keywords: Vec<String>,

    /// Keyword→emoji map (e.g. `rail` → `"🚆"`). Surfaced in the classifier
    /// bundle as a comment so the migrating maintainer can fold it back into a
    /// post template later.
    #[serde(default)]
    pub emoji_map: BTreeMap<String, String>,

    /// The Frankie digest title (e.g. `"🗳️ Transportation Bills Weekly
    /// Digest"`). Becomes the RSS/HTML publisher title.
    pub digest_title: Option<String>,

    /// The Frankie "topic focus" string — a short framing the summarizer
    /// uses (e.g. `"transportation"`).
    pub topic: Option<String>,

    /// Catch-all for fields Frankie carries that this migration tool does not
    /// translate. Held so unknown fields don't fail the parse.
    #[serde(flatten)]
    pub extra: serde_yaml::Value,
}

impl FrankieTopicConfig {
    /// Parse a Frankie-style `topics/<name>/config.yml` from a path.
    pub fn load(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("Failed to read Frankie config: {}", path.display()))?;
        let parsed: Self = serde_yaml::from_str(&contents)
            .with_context(|| format!("Failed to parse Frankie config: {}", path.display()))?;
        if parsed.name.trim().is_empty() {
            return Err(anyhow!(
                "Frankie config {} has empty `name` — required to scaffold a classifier tag",
                path.display()
            ));
        }
        Ok(parsed)
    }

    /// The human-readable display name. Falls back to title-casing `name`
    /// (e.g. `transportation` → `Transportation`).
    pub fn display(&self) -> String {
        self.display_name.clone().unwrap_or_else(|| {
            let mut chars = self.name.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
    }

    /// The summarizer-prompt topic focus, defaulting to the lowercased name.
    pub fn topic_focus(&self) -> String {
        self.topic.clone().unwrap_or_else(|| self.name.clone())
    }
}

/// Scaffold a govbot+fastclass project at `into` from a parsed Frankie config.
/// Returns the absolute path the project was written to.
pub fn scaffold(config: &FrankieTopicConfig, into: &Path) -> Result<PathBuf> {
    // Pre-flight guard: refuse to clobber an existing project.
    let manifest_path = into.join("govbot.yml");
    if manifest_path.exists() {
        return Err(anyhow!(
            "{} already exists — refusing to overwrite an existing govbot project. \
             Remove it first or scaffold into a different directory with --into <path>.",
            manifest_path.display()
        ));
    }

    fs::create_dir_all(into)
        .with_context(|| format!("Failed to create scaffold dir: {}", into.display()))?;

    // 1. govbot.yml manifest
    fs::write(&manifest_path, render_govbot_yml(config))
        .with_context(|| format!("Failed to write {}", manifest_path.display()))?;

    // 2. classifier bundle
    let classifier_dir = into.join("classifier");
    fs::create_dir_all(&classifier_dir)?;
    fs::write(
        classifier_dir.join("classifier.yml"),
        render_classifier_yml(config),
    )?;
    fs::write(classifier_dir.join("fusion.yml"), render_fusion_yml())?;

    let eval_dir = classifier_dir.join("eval");
    fs::create_dir_all(&eval_dir)?;
    fs::write(
        eval_dir.join("constitution.yml"),
        render_constitution_yml(config),
    )?;
    fs::write(eval_dir.join("rolling.yml"), render_rolling_yml())?;

    // proposals dir — empty; the improvement loop populates it.
    fs::create_dir_all(classifier_dir.join("proposals"))?;
    // Keep the dir tracked even though it is empty today.
    fs::write(classifier_dir.join("proposals").join(".gitkeep"), "")?;

    // 3. summarizer stub
    let summarizer_dir = into.join("summarizer");
    fs::create_dir_all(&summarizer_dir)?;
    fs::write(
        summarizer_dir.join("prompt.md"),
        render_summarizer_prompt(config),
    )?;

    // 4. README
    fs::write(into.join("README.md"), render_readme(config))?;

    // 5. .gitignore
    fs::write(into.join(".gitignore"), render_gitignore())?;

    Ok(into.to_path_buf())
}

/// Run the full `--from-frankie-config <path> [--into <dir>]` flow: parse,
/// scaffold, and print activist-facing next-steps to stdout.
pub fn run(from_config: &Path, into: Option<&Path>) -> Result<()> {
    let config = FrankieTopicConfig::load(from_config)?;
    let cwd = std::env::current_dir()?;
    let into_path: PathBuf = into.map(|p| p.to_path_buf()).unwrap_or(cwd);
    let written = scaffold(&config, &into_path)?;
    print_next_steps(&written, from_config);
    Ok(())
}

fn print_next_steps(into: &Path, from_config: &Path) {
    println!(
        "✓ Scaffolded govbot+fastclass project at {}.",
        into.display()
    );
    println!();
    println!(
        "This project was created from {}. The keyword list became",
        from_config.display()
    );
    println!("your starter classifier; everything else is yours to refine.");
    println!();
    println!("Recommended next steps:");
    println!();
    println!("  1. Install the Tier-2 semantic model so embedding matchers fire:");
    println!("     fastclass model fetch --bundle ./classifier");
    println!();
    println!("  2. Replace the placeholder constitution items with real labeled examples:");
    println!("     /fastclass:seed-gold ./classifier");
    println!();
    println!("  3. Try classifying:");
    println!("     govbot run --dry-run");
    println!();
    println!("  4. Iterate quality via the improvement loop:");
    println!("     /fastclass:improve autonomous");
    println!();
    println!("  5. Set Bluesky credentials (env-only — never in govbot.yml):");
    println!("     export BLUESKY_HANDLE=...");
    println!("     export BLUESKY_APP_PASSWORD=...");
}

// ---------------------------------------------------------------------------
// File renderers — each pure function takes the Frankie config and returns
// the file contents. Keeping rendering pure makes unit testing trivial.
// ---------------------------------------------------------------------------

fn render_govbot_yml(config: &FrankieTopicConfig) -> String {
    let display = config.display();
    let title = config
        .digest_title
        .clone()
        .unwrap_or_else(|| format!("{} Bills Weekly Digest", display));

    let mut yml = String::new();
    yml.push_str("# Govbot manifest — scaffolded from a Frankie-style topic config.\n");
    yml.push_str("# See README.md for the migration story. Tune the classifier bundle\n");
    yml.push_str("# (./classifier) with the fastclass improvement loop, not by hand.\n");
    yml.push_str("$schema: https://raw.githubusercontent.com/chihacknight/govbot/main/schemas/govbot.schema.json\n\n");

    yml.push_str("datasets:\n");
    yml.push_str("  - all\n\n");

    yml.push_str("transforms:\n");
    yml.push_str("  classify:\n");
    yml.push_str("    command: [fastclass, classify, \"-\"]\n");
    yml.push_str("    reads: docs\n");
    yml.push_str("    writes: classification\n");
    yml.push_str("    classifier: ./classifier\n\n");

    yml.push_str("publish:\n");
    yml.push_str("  feed:\n");
    yml.push_str("    type: rss\n");
    yml.push_str(&format!("    select: [{}]\n", config.name));
    yml.push_str(&format!("    title: {}\n", yaml_string(&title)));
    yml.push_str("    base_url: \"https://example.org/your-deployment\"\n");
    yml.push_str("    output_dir: dist\n");
    yml.push_str(&format!("    output_file: {}-feed.xml\n\n", config.name));

    yml.push_str("  site:\n");
    yml.push_str("    type: html\n");
    yml.push_str(&format!("    select: [{}]\n", config.name));
    yml.push_str(&format!("    title: {}\n", yaml_string(&title)));
    yml.push_str("    base_url: \"https://example.org/your-deployment\"\n");
    yml.push_str("    output_dir: dist\n\n");

    yml.push_str("  bluesky:\n");
    yml.push_str("    type: bluesky\n");
    yml.push_str(&format!("    select: [{}]\n", config.name));
    yml.push_str("    # Calibrated final_score threshold (0..1). 0.55 is a sensible starting\n");
    yml.push_str("    # point per the climate-activist deployment; raise to cut false\n");
    yml.push_str("    # positives, lower to widen recall.\n");
    yml.push_str("    min_score: 0.55\n");
    yml.push_str("    base_url: \"https://example.org/your-deployment\"\n");
    yml.push_str("    post_template: \"{title}\\n\\n{tags} · {link}\"\n");
    yml.push_str("    # Credentials are env-only: BLUESKY_HANDLE / BLUESKY_APP_PASSWORD.\n");
    yml.push_str("    # Never put them in this file.\n\n");

    yml.push_str("pipelines:\n");
    yml.push_str("  default:\n");
    yml.push_str("    - classify\n");
    yml.push_str("    - feed\n");
    yml.push_str("    - site\n");
    yml.push_str("    - bluesky\n");

    yml
}

fn render_classifier_yml(config: &FrankieTopicConfig) -> String {
    let display = config.display();
    let topic_focus = config.topic_focus();

    let mut yml = String::new();
    yml.push_str("# classifier.yml — the taxonomy for this govbot+fastclass project.\n");
    yml.push_str("#\n");
    yml.push_str("# Scaffolded from a Frankie-style topic config. The single tag below\n");
    yml.push_str("# carries the keyword list from that config verbatim; everything else\n");
    yml.push_str("# (exclude gates, regex, examples, HyDE queries, subjects) is yours\n");
    yml.push_str("# to grow via the /fastclass:improve loop. Never hand-tune by guessing;\n");
    yml.push_str("# every change should be proved against the frozen gold set in\n");
    yml.push_str("# eval/constitution.yml.\n");
    if !config.emoji_map.is_empty() {
        yml.push_str("#\n");
        yml.push_str("# Frankie emoji_map (kept here for reference — fold into a post template\n");
        yml.push_str("# later if you want per-subdomain emoji in Bluesky posts):\n");
        for (keyword, emoji) in &config.emoji_map {
            yml.push_str(&format!("#   {} → {}\n", keyword, emoji));
        }
    }
    yml.push_str("tags:\n");
    yml.push_str(&format!("  {}:\n", config.name));
    yml.push_str(&format!(
        "    description: >-\n      Bills about {} — scaffolded from the Frankie topic\n      config for \"{}\". Refine this description as the tag evolves.\n",
        topic_focus, display
    ));
    yml.push_str("    include_keywords:\n");
    for keyword in &config.keywords {
        yml.push_str(&format!("      - {}\n", yaml_string(keyword)));
    }
    yml.push_str("    # examples are intentionally empty — add real labeled bills via\n");
    yml.push_str("    # /fastclass:from-intent or by curating eval/constitution.yml.\n");
    yml.push_str("    examples: []\n");
    yml.push_str("    threshold: 0.3\n");

    yml
}

fn render_fusion_yml() -> String {
    // Mirrors the climate-activist bundle's fusion.yml — the portable
    // `models:` block declares the encoder + reranker so `fastclass model
    // fetch --bundle ./classifier` resolves and installs them.
    let mut yml = String::new();
    yml.push_str("# fusion.yml — global fusion config for the classifier bundle.\n");
    yml.push_str("# Owned by fastclass. Per-tag overrides live INLINE in classifier.yml.\n");
    yml.push_str("version: fusion-v1\n\n");
    yml.push_str(
        "# Portable model declaration. Run `fastclass model fetch --bundle ./classifier`\n",
    );
    yml.push_str("# to install these into the shared ~/.govbot/models/<sha>/ cache.\n");
    yml.push_str("models:\n");
    yml.push_str("  encoder: sentence-transformers/all-MiniLM-L6-v2\n");
    yml.push_str("  reranker: cross-encoder/ms-marco-MiniLM-L-6-v2\n\n");
    yml.push_str("# Default fusion weight per matcher kind, applied to any tag that does not\n");
    yml.push_str("# declare its own inline `fusion_weights`.\n");
    yml.push_str("weights:\n");
    yml.push_str("  keyword: 1.0\n");
    yml.push_str("  regex: 0.8\n\n");
    yml.push_str("# Cascade uncertainty band. Documents whose fused score lands inside\n");
    yml.push_str("# [low, high] are the uncertain ones the improvement loop focuses on.\n");
    yml.push_str("uncertainty_band:\n");
    yml.push_str("  low: 0.3\n");
    yml.push_str("  high: 0.7\n\n");
    yml.push_str("splitters:\n");
    yml.push_str("  default:\n");
    yml.push_str("    strategy: whole\n");
    yml.push_str("  sections:\n");
    yml.push_str("    strategy: sections\n");
    yml.push_str("    aggregation: max\n");
    yml
}

fn render_constitution_yml(config: &FrankieTopicConfig) -> String {
    // PLACEHOLDER items per the seed-gold pattern, clearly marked. The
    // activist replaces them with real labeled bills via /fastclass:seed-gold.
    let mut yml = String::new();
    yml.push_str("# constitution.yml — the FROZEN gold standard for this classifier.\n");
    yml.push_str("# Never shown to an LLM. The items below are PLACEHOLDERS — replace them\n");
    yml.push_str("# with real labeled bills (use /fastclass:seed-gold ./classifier) before\n");
    yml.push_str("# relying on the improvement loop's judgement.\n");
    yml.push_str("items:\n");
    yml.push_str(&format!("  - id: placeholder-{}-positive\n", config.name));
    yml.push_str("    text: >-\n");
    yml.push_str(&format!(
        "      PLACEHOLDER — replace with a real {} bill the classifier should\n      tag (positive example).\n",
        config.topic_focus()
    ));
    yml.push_str(&format!("    expected_tags: [{}]\n", config.name));
    yml.push_str(&format!("  - id: placeholder-{}-negative\n", config.name));
    yml.push_str("    text: >-\n");
    yml.push_str(&format!(
        "      PLACEHOLDER — replace with a real bill that should NOT be tagged\n      {} (negative example used to gate false-positives).\n",
        config.name
    ));
    yml.push_str("    expected_tags: []\n");
    yml
}

fn render_rolling_yml() -> String {
    let mut yml = String::new();
    yml.push_str("# rolling.yml — the refreshable working eval set.\n");
    yml.push_str("# The improvement loop adds failing bills here and proves fixes against\n");
    yml.push_str("# the (unseen) constitution. Empty today — start by labeling a handful\n");
    yml.push_str("# of bills from `govbot source --select docs` you disagree with.\n");
    yml.push_str("items: []\n");
    yml
}

fn render_summarizer_prompt(config: &FrankieTopicConfig) -> String {
    let display = config.display();
    let topic = config.topic_focus();
    let mut s = String::new();
    s.push_str(&format!("# {} summarizer prompt (stub)\n\n", display));
    s.push_str(&format!(
        "Describe this bill in one neutral sentence, focused on {} policy.\n",
        topic
    ));
    s.push_str(
        "Avoid editorial language; let the bill text speak for itself. \
         A future `summarize` transform will read this prompt — today it is a\n\
         placeholder for the migrating maintainer to refine.\n",
    );
    s
}

fn render_readme(config: &FrankieTopicConfig) -> String {
    let display = config.display();
    let emoji = config.default_emoji.as_deref().unwrap_or("");
    let topic = config.topic_focus();

    let mut s = String::new();
    s.push_str(&format!("# {} {} govbot deployment\n\n", emoji, display));
    s.push_str(
        "This is a govbot+fastclass project scaffolded **from a Frankie-style\n\
         topic config**. The Frankie keyword list became your starter classifier;\n\
         everything else is yours to refine.\n\n",
    );
    s.push_str("## What was generated\n\n");
    s.push_str("- `govbot.yml` — the project manifest (datasets, transforms, publishers).\n");
    s.push_str(&format!(
        "- `classifier/` — a fastclass bundle with one tag (`{}`) carrying the\n  Frankie keyword list as `include_keywords`.\n",
        config.name
    ));
    s.push_str("- `classifier/eval/constitution.yml` — **placeholder** gold items;\n");
    s.push_str("  replace before relying on the improvement loop's judgement.\n");
    s.push_str("- `summarizer/prompt.md` — stub for the future `summarize` transform.\n\n");
    s.push_str("## What to do next\n\n");
    s.push_str("1. Install the Tier-2 semantic model:\n");
    s.push_str("   ```\n   fastclass model fetch --bundle ./classifier\n   ```\n");
    s.push_str("2. Replace the placeholder constitution items with real labeled examples:\n");
    s.push_str("   ```\n   /fastclass:seed-gold ./classifier\n   ```\n");
    s.push_str("3. Try classifying:\n");
    s.push_str("   ```\n   govbot run --dry-run\n   ```\n");
    s.push_str("4. Iterate quality via the improvement loop:\n");
    s.push_str("   ```\n   /fastclass:improve autonomous\n   ```\n");
    s.push_str("5. Set Bluesky credentials (env-only — never in `govbot.yml`):\n");
    s.push_str(
        "   ```\n   export BLUESKY_HANDLE=...\n   export BLUESKY_APP_PASSWORD=...\n   ```\n\n",
    );
    s.push_str(&format!(
        "## Topic focus\n\n`{}` — used by the summarizer prompt and the tag\ndescription. Adjust as your editorial scope sharpens.\n",
        topic
    ));
    s
}

fn render_gitignore() -> String {
    "# govbot — generated, reconstructed on every run\n\
     .govbot/\n\
     dist/\n\
     docs/\n\
     # Classification output from `govbot apply` — regenerated each run.\n\
     tags/\n\
     # Publisher state — append-only ledgers.\n\
     state/\n\
     # fastclass / govbot lockfiles\n\
     fastclass.lock\n\
     govbot.lock\n\
     # Bundled model artifacts (resolved by `fastclass model fetch`).\n\
     classifier/model/\n\
     classifier/model-rerank/\n\
     \n\
     # Secrets — never commit\n\
     .env\n"
        .to_string()
}

/// Quote a YAML scalar conservatively — escapes any embedded `"` and wraps in
/// double quotes. Used for keyword lines and titles, where the source can
/// carry characters that would otherwise confuse the YAML parser.
fn yaml_string(s: &str) -> String {
    let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{}\"", escaped)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// The parser must accept a minimal Frankie config and tolerate extra
    /// fields the migration tool does not translate.
    #[test]
    fn frankie_config_parser_handles_minimal_config_with_extras() {
        let yml = r#"
name: housing
display_name: Housing
default_emoji: 🏠
keywords:
  - affordable housing
  - rent control
  - eviction
emoji_map:
  rent: 💵
  eviction: 🚪
digest_title: "🏠 Housing Bills Weekly Digest"
topic: "housing policy"
# Extras Frankie carries that we don't translate yet:
schedule: weekly
timezone: America/Chicago
jurisdictions:
  - il
  - ca
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yml");
        std::fs::write(&path, yml).unwrap();

        let parsed = FrankieTopicConfig::load(&path).expect("minimal Frankie config should parse");
        assert_eq!(parsed.name, "housing");
        assert_eq!(parsed.display(), "Housing");
        assert_eq!(parsed.default_emoji.as_deref(), Some("🏠"));
        assert_eq!(parsed.keywords.len(), 3);
        assert_eq!(parsed.emoji_map.get("rent").map(String::as_str), Some("💵"));
        assert_eq!(parsed.topic_focus(), "housing policy");
        // Extra fields are absorbed, not rejected.
        match parsed.extra {
            serde_yaml::Value::Mapping(m) => {
                assert!(m.contains_key(serde_yaml::Value::String("schedule".to_string())));
                assert!(m.contains_key(serde_yaml::Value::String("jurisdictions".to_string())));
            }
            other => panic!("expected extras to land in a mapping, got: {:?}", other),
        }
    }

    /// Display falls back to title-casing `name` when `display_name` is absent.
    #[test]
    fn display_falls_back_to_title_case() {
        let cfg = FrankieTopicConfig {
            name: "transportation".to_string(),
            display_name: None,
            default_emoji: None,
            keywords: vec![],
            emoji_map: BTreeMap::new(),
            digest_title: None,
            topic: None,
            extra: serde_yaml::Value::Null,
        };
        assert_eq!(cfg.display(), "Transportation");
    }

    /// Empty `name` is rejected — the classifier needs a tag name.
    #[test]
    fn frankie_config_rejects_empty_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yml");
        std::fs::write(&path, "name: \"\"\nkeywords: []\n").unwrap();

        let err = FrankieTopicConfig::load(&path).expect_err("empty name must be rejected");
        assert!(err.to_string().contains("empty `name`"));
    }
}
