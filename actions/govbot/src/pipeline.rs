use crate::config::{Command_, Manifest, Transform};
use crate::git::repo_dir_name;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Run the full govbot pipeline against the project's `govbot.yml`.
///
/// Stages:
///  1. **pull/update** — clone or git-pull the manifest's `datasets`.
///  2. **classify+apply** — the transform DAG: stream `source --select docs`
///     into each declared transform (an external process speaking the govbot
///     stream protocol) and pipe the final transform's output into
///     `govbot apply`.
///  3. **publish** — run `govbot publish` to emit the manifest's publishers.
///
/// `dry_run` is passed through to step 3 so publishers render but do not
/// emit; the `bluesky` publisher in particular honours it by touching no
/// network and no ledger.
///
/// Smart update behavior: if `<govbot_dir>/repos/` already has datasets, just
/// `git pull`; otherwise clone the manifest's `datasets`.
pub fn run_pipeline(config_path: &Path, govbot_dir: Option<&str>, dry_run: bool) -> Result<()> {
    let govbot_bin = std::env::current_exe().context("Failed to determine govbot binary path")?;

    let cwd = config_path.parent().unwrap_or_else(|| Path::new("."));

    let manifest = Manifest::load(config_path)?;

    // The transforms govbot runs in step 2. If the manifest declares no
    // pipeline, fall back to the classic single classify-transform DAG (a
    // `fastclass classify` stage with the classifier bundle at `.`).
    let transforms = resolve_pipeline_transforms(&manifest)?;

    // Fast-fail if a transform's binary cannot be resolved.
    let resolved: Vec<(String, ResolvedTransform)> = transforms
        .iter()
        .map(|(name, t)| resolve_transform(t).map(|r| (name.clone(), r)))
        .collect::<Result<_>>()?;

    // Resolve the repos directory the way subcommands do.
    let repos_dir = match govbot_dir {
        Some(d) => Path::new(d).join("repos"),
        None => cwd.join(".govbot").join("repos"),
    };
    let has_repos = repos_dir.exists()
        && std::fs::read_dir(&repos_dir)
            .map(|mut d| d.next().is_some())
            .unwrap_or(false);

    // Classify each manifest dataset: is the project-local seed already
    // populated for it? If every declared dataset has a non-empty
    // project-local directory under `repos/`, the pull substep is a no-op —
    // skip it (and the shared `~/.govbot/cache/` write the registry-driven
    // pull would attempt) so a sandbox / read-only HOME does not error out a
    // run that has all the data it needs sitting right there.
    let locally_seeded: Vec<&String> = manifest
        .datasets
        .iter()
        .filter(|name| name.as_str() != "all" && is_local_seed(&repos_dir, name))
        .collect();
    let all_locally_seeded =
        !manifest.datasets.is_empty() && locally_seeded.len() == manifest.datasets.len();

    // Step 1: pull or update datasets.
    eprintln!();
    eprintln!(
        "=== Step 1/3: {} datasets ===",
        if all_locally_seeded {
            "Using local seed for"
        } else if has_repos {
            "Updating"
        } else {
            "Pulling"
        }
    );
    eprintln!();

    if all_locally_seeded {
        for name in &locally_seeded {
            let seed = repos_dir.join(seed_dir_name(name));
            eprintln!("📂 using local seed: {}", seed.display());
        }
        // Skip the cache-touching pull subprocess entirely.
    } else {
        let pull_status = {
            let mut cmd = Command::new(&govbot_bin);
            cmd.arg("pull");
            if !has_repos {
                // Initial pull: clone the manifest's datasets.
                for dataset in &manifest.datasets {
                    cmd.arg(dataset);
                }
            }
            if let Some(d) = govbot_dir {
                cmd.arg("--govbot-dir").arg(d);
            }
            cmd.current_dir(cwd)
                .stdin(Stdio::inherit())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .status()
        };
        match pull_status {
            Ok(status) if !status.success() => {
                eprintln!("⚠️  Pull/update had errors (continuing anyway)");
            }
            Err(e) => {
                eprintln!("⚠️  Failed to run pull: {} (continuing anyway)", e);
            }
            _ => {}
        }
    }

    // Step 2: run the transform DAG (source | transform... | apply).
    //
    // The source stage must honour the manifest's `datasets:` scope — without
    // it, `govbot source` walks every linked dataset under `.govbot/repos/`
    // (which can include datasets pulled by an earlier `datasets: [all]` run
    // and never cleaned up), classifying tens of thousands of records the
    // current manifest did not declare.
    let source_repos = source_repos_from_manifest(&manifest.datasets);
    eprintln!();
    eprintln!("=== Step 2/3: Running transforms (source | ... | apply) ===");
    eprintln!();
    match run_transform_dag(&govbot_bin, &resolved, cwd, govbot_dir, &source_repos) {
        Ok(false) => {
            eprintln!("⚠️  Transform stage had errors (continuing anyway)");
        }
        Err(e) => {
            eprintln!("⚠️  Failed to run transforms: {} (continuing anyway)", e);
        }
        _ => {}
    }

    // Step 3: publish.
    eprintln!();
    eprintln!("=== Step 3/3: Publishing ===");
    eprintln!();
    let mut publish_cmd = Command::new(&govbot_bin);
    publish_cmd.arg("publish");
    if let Some(d) = govbot_dir {
        publish_cmd.arg("--govbot-dir").arg(d);
    }
    if dry_run {
        publish_cmd.arg("--dry-run");
    }
    let publish_status = publish_cmd
        .current_dir(cwd)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .context("Failed to run govbot publish")?;
    if !publish_status.success() {
        anyhow::bail!(
            "Publish step failed with exit code: {}",
            publish_status.code().unwrap_or(-1)
        );
    }

    eprintln!();
    eprintln!("Pipeline complete!");
    Ok(())
}

/// Resolve which transforms `govbot run` executes.
///
/// If the manifest declares pipelines, the first pipeline's stages that name a
/// `transforms:` entry are run, in order. (Publisher stages are handled by the
/// separate `publish` step.) If no pipeline / no transforms are declared, fall
/// back to a single `fastclass classify` transform with the classifier bundle
/// at `.` (the project directory).
fn resolve_pipeline_transforms(manifest: &Manifest) -> Result<Vec<(String, Transform)>> {
    // Prefer an explicit pipeline; pick the first one deterministically.
    if let Some((_, stages)) = manifest.pipelines.iter().next() {
        let mut out = Vec::new();
        for stage in stages {
            if let Some(t) = manifest.transforms.get(stage) {
                out.push((stage.clone(), t.clone()));
            }
            // A stage naming a publisher is handled by the publish step;
            // a stage naming neither is a manifest error surfaced elsewhere.
        }
        if !out.is_empty() {
            return Ok(out);
        }
    }

    // No pipeline transforms: run every declared transform, in name order.
    if !manifest.transforms.is_empty() {
        return Ok(manifest
            .transforms
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect());
    }

    // Nothing declared — the classic single classify transform. The classifier
    // bundle defaults to `.` (the project directory holding the bundle).
    Ok(vec![(
        "classify".to_string(),
        Transform {
            command: Command_::Argv(vec![
                "fastclass".to_string(),
                "classify".to_string(),
                "-".to_string(),
            ]),
            reads: "docs".to_string(),
            writes: "classification".to_string(),
            classifier: Some(".".to_string()),
        },
    )])
}

/// A transform whose binary has been resolved to an absolute path, with its
/// full argv assembled (including any `classifier=<bundle>` argument).
struct ResolvedTransform {
    /// The resolved executable path.
    bin: PathBuf,
    /// Arguments passed after the executable.
    args: Vec<String>,
}

/// Resolve a transform's command to an executable + argv.
///
/// The first argv element is the binary, resolved against `$PATH` and the
/// standard install locations (`~/.cargo/bin`, `~/.govbot/bin`). For a
/// classify-style transform the `classifier=<bundle>` field is appended as an
/// explicit argument — NOT hard-coded to the cwd.
fn resolve_transform(t: &Transform) -> Result<ResolvedTransform> {
    let argv = t.command.argv();
    let (bin_name, rest) = argv.split_first().context("transform `command` is empty")?;

    let bin = resolve_transform_binary(bin_name).ok_or_else(|| {
        anyhow::anyhow!(
            "transform binary `{}` not found on PATH, ~/.cargo/bin, or ~/.govbot/bin.\n\
             For the bundled classify transform, install fastclass:\n\
               cd <fastclass repo> && just install   (or: cargo install --path .)",
            bin_name
        )
    })?;

    let mut args: Vec<String> = rest.to_vec();
    // Append the explicit classifier bundle path for classify-style transforms.
    if let Some(classifier) = &t.classifier {
        args.push(format!("classifier={}", classifier));
    }

    Ok(ResolvedTransform { bin, args })
}

/// The user's home directory, from `$HOME` (Unix) or `%USERPROFILE%` (Windows).
fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
}

/// Resolve a transform binary by name: `$PATH` first, then the standard install
/// locations (`~/.cargo/bin`, `~/.govbot/bin`). An absolute/relative path that
/// already exists is used as-is. This generalizes the old `find_fastclass()`.
fn resolve_transform_binary(name: &str) -> Option<PathBuf> {
    // An explicit path component — use it directly if it resolves.
    if name.contains('/') || name.contains('\\') {
        let p = PathBuf::from(name);
        return p.is_file().then_some(p);
    }

    let exe = if cfg!(windows) && !name.ends_with(".exe") {
        format!("{}.exe", name)
    } else {
        name.to_string()
    };

    if let Ok(path) = std::env::var("PATH") {
        if let Some(hit) = std::env::split_paths(&path)
            .map(|p| p.join(&exe))
            .find(|p| p.is_file())
        {
            return Some(hit);
        }
    }
    let home = home_dir()?;
    [".cargo/bin", ".govbot/bin"]
        .into_iter()
        .map(|d| home.join(d).join(&exe))
        .find(|p| p.is_file())
}

/// Run the transform DAG: `govbot source --select docs | <t1> | <t2> | ... |
/// govbot apply`.
///
/// A **linear executor** — each transform is an external process speaking the
/// govbot stream protocol (newline-delimited JSON, `{id,text,kind}` in,
/// results out). Output of stage N is piped to the stdin of stage N+1. The
/// `transforms:`/`pipelines:` schema is DAG-capable; this runner walks it
/// linearly, which is sufficient for the single-classifier pipeline today.
///
/// Returns `Ok(true)` when every stage exits successfully.
fn run_transform_dag(
    govbot_bin: &Path,
    transforms: &[(String, ResolvedTransform)],
    cwd: &Path,
    govbot_dir: Option<&str>,
    source_repos: &[String],
) -> Result<bool> {
    // Stage 0: the source — `govbot source --select docs`. Scope it to the
    // manifest's declared datasets (an empty list means "every linked
    // dataset", matching the standalone `govbot source` default).
    let mut source_cmd = Command::new(govbot_bin);
    source_cmd.arg("source").arg("--select").arg("docs");
    if let Some(d) = govbot_dir {
        source_cmd.arg("--govbot-dir").arg(d);
    }
    if !source_repos.is_empty() {
        source_cmd.arg("--repos");
        for d in source_repos {
            source_cmd.arg(d);
        }
    }
    let mut source_child = source_cmd
        .current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("Failed to spawn govbot source")?;
    let mut prev_stdout: Stdio = source_child
        .stdout
        .take()
        .context("Failed to capture source stdout")?
        .into();

    // Each transform stage reads the previous stage's stdout.
    let mut transform_children = Vec::new();
    for (name, t) in transforms {
        let mut child = Command::new(&t.bin)
            .args(&t.args)
            .current_dir(cwd)
            .stdin(prev_stdout)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("Failed to spawn transform '{}'", name))?;
        prev_stdout = child
            .stdout
            .take()
            .with_context(|| format!("Failed to capture stdout of transform '{}'", name))?
            .into();
        transform_children.push(child);
    }

    // The sink: `govbot apply` consumes the final transform's result stream.
    let apply_child = Command::new(govbot_bin)
        .arg("apply")
        .current_dir(cwd)
        .stdin(prev_stdout)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .context("Failed to spawn govbot apply")?;

    // Wait downstream-to-upstream so pipes drain.
    let apply_output = apply_child
        .wait_with_output()
        .context("Failed to wait for govbot apply")?;
    let mut all_ok = apply_output.status.success();
    let mut statuses: HashMap<String, bool> = HashMap::new();
    for (child, (name, _)) in transform_children.iter_mut().zip(transforms.iter()) {
        let status = child
            .wait()
            .with_context(|| format!("Failed to wait for transform '{}'", name))?;
        statuses.insert(name.clone(), status.success());
        all_ok &= status.success();
    }
    let source_status = source_child
        .wait()
        .context("Failed to wait for govbot source")?;
    all_ok &= source_status.success();

    Ok(all_ok)
}

/// Map a manifest dataset id to the on-disk directory name under `repos/`.
///
/// A manifest id can be a bare jurisdiction code (`wy`) — which the registry
/// resolves to a `short_name`, then `repo_dir_name` suffixes (`wy-legislation`)
/// — or it can already match the on-disk dir name. We try the suffixed form
/// first; the raw id is the fallback for the (rare) namespaced-id case.
fn seed_dir_name(manifest_id: &str) -> String {
    // Strip a `namespace/` prefix (`us-legislation/wy` -> `wy`) so the
    // suffixed form matches `wy-legislation`.
    let bare = manifest_id.rsplit('/').next().unwrap_or(manifest_id);
    repo_dir_name(bare)
}

/// True when `<repos_dir>/<seed_dir_name(name)>/` (or the raw name) exists and
/// has at least one entry. The directory walks `govbot source` does for the
/// dataset will succeed iff this is the case.
fn is_local_seed(repos_dir: &Path, manifest_id: &str) -> bool {
    let candidate1 = repos_dir.join(seed_dir_name(manifest_id));
    let candidate2 = repos_dir.join(manifest_id);
    [candidate1, candidate2]
        .into_iter()
        .any(|p| dir_has_entries(&p))
}

/// True when `p` is a directory (or a symlink resolving to one) with at least
/// one child entry.
fn dir_has_entries(p: &Path) -> bool {
    std::fs::read_dir(p)
        .map(|mut it| it.next().is_some())
        .unwrap_or(false)
}

/// Translate the manifest's `datasets:` list to the `--repos` argv that
/// scopes the `govbot source` stage inside `run_transform_dag`.
///
/// `datasets: [all]` becomes an empty list — `govbot source`'s own sentinel
/// for "every linked dataset", omitted from the argv so the flag is absent.
/// Any other list is passed through verbatim; `govbot source --repos <list>`
/// then walks only the named datasets.
///
/// This is the load-bearing piece of [`run_pipeline`]'s step 2: forgetting
/// to pass `--repos` here caused a bug in which a manifest declaring
/// `datasets: [wy]` still classified ~4900 records across 52 states because
/// the cache held datasets from an earlier `[all]` pull.
fn source_repos_from_manifest(datasets: &[String]) -> Vec<String> {
    if datasets == ["all"] {
        Vec::new()
    } else {
        datasets.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test for the `datasets:[wy]` scope leak: an `[all]`
    /// manifest must produce an empty argv list (so `--repos` is omitted —
    /// `govbot source`'s sentinel for "every linked dataset"), but any other
    /// list must pass through verbatim so `govbot source --repos <list>`
    /// scopes the walk. Pre-fix, `run_transform_dag` never passed `--repos`,
    /// so the manifest's `datasets:` was silently ignored at the source step
    /// and a `[wy]` manifest still classified ~4900 records across 52 states.
    #[test]
    fn source_repos_from_manifest_translates_all_and_scopes() {
        assert_eq!(
            source_repos_from_manifest(&["all".to_string()]),
            Vec::<String>::new(),
            "`[all]` must collapse to empty so --repos is omitted"
        );
        assert_eq!(
            source_repos_from_manifest(&["wy".to_string()]),
            vec!["wy".to_string()],
            "`[wy]` must pass through verbatim"
        );
        assert_eq!(
            source_repos_from_manifest(&["wy".to_string(), "il".to_string()]),
            vec!["wy".to_string(), "il".to_string()],
            "`[wy, il]` must pass through verbatim"
        );
        // An `[all, wy]` mix is not the `[all]` sentinel — pass through so
        // the source step at least scopes to the named subset (and treats
        // the literal `all` as a possibly-missing dataset id, surfacing the
        // manifest error rather than silently widening to every dataset).
        assert_eq!(
            source_repos_from_manifest(&["all".to_string(), "wy".to_string()]),
            vec!["all".to_string(), "wy".to_string()],
        );
    }

    /// `govbot run` should detect a project-local dataset seed
    /// (`.govbot/repos/<short>/`) and skip the cache-touching pull substep.
    /// We test the detector — the substep skip itself is exercised by the
    /// integration repro in the bug 3 PR description.
    #[test]
    fn is_local_seed_detects_populated_dir() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let repos = tmp.path();
        // Empty repos/: no seed.
        assert!(!is_local_seed(repos, "wy"));

        // Create the expected dataset dir with a file inside.
        let seed = repos.join("wy-legislation");
        std::fs::create_dir_all(&seed).unwrap();
        std::fs::write(seed.join("data.json"), b"{}").unwrap();
        assert!(is_local_seed(repos, "wy"));

        // Namespaced id — still finds the suffixed dir.
        assert!(is_local_seed(repos, "us-legislation/wy"));
    }
}
