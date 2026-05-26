# pipeline-manager — agent playbook

Read this before editing anything in `actions/pipeline-manager/`. It is the playbook for the **data catalog layer**: the declarative YAMLs + Python orchestration that ship workflow code to the per-jurisdiction repos which actually produce govbot's legislative data.

If you're a human, the root `AGENT.md` is the right entrypoint — this file assumes you're already oriented to the four-tool govbot stack.

## 1. Mental model

This directory is **a declarative repo factory**, not a runtime scraper.

It reads two YAML catalogs (`chn-openstates-scrape.yml`, `chn-openstates-files.yml`), renders workflow templates per locale into `generated/`, and reconciles the resulting set of per-state repos against a GitHub org — create missing, update drifted, delete orphans.

The actual scraping/formatting happens **inside the generated GitHub Actions workflows in those per-state repos**. This directory doesn't run scrapers; it ships workflow YAML to repos that do.

Two orgs are in play:

- **`chn-openstates-scrapers`** — raw OpenStates output, one repo per jurisdiction. Driven by `chn-openstates-scrape.yml`.
- **`chn-openstates-files`** (a.k.a. `govbot-data` post-rename) — OCD-formatted data, one repo per jurisdiction. Driven by `chn-openstates-files.yml`. Triggered from the scraper repo via `repository_dispatch: scrape-and-format-complete`.

The `chn-openstates-files` org is what `govbot pull` actually reads — it's the user-visible side.

## 2. The two configs, side-by-side

| File | What it manages | Per-locale knobs |
|---|---|---|
| `chn-openstates-scrape.yml` | Scraper repos (OpenStates → raw output) | `template`, `toolkit_branch`, `name`, `disabled_jobs`, `labels` |
| `chn-openstates-files.yml` | Formatter repos (raw → OCD `metadata.json` + logs) | same |

The `labels: [working]` flag on `chn-openstates-files.yml` is what **gates user-visible publication**. A jurisdiction missing that label ships empty/broken data even if the entry exists. As of 2026-05 the gaps are AZ, CT, TX, VA on the files side (chihacknight/govbot#33) and ~19 jurisdictions on the scrape side.

`disabled_jobs:` is a list of workflow filenames (without extension) to skip rendering — most locales disable `extract-text` because text extraction isn't wired up yet.

## 3. Python orchestration — read these in this order

For any change beyond editing a single YAML line:

1. **`render.py`** — parses YAML, walks locales, does sed-style `✏️{ var }✏️` substitution into `generated/<config-stem>/<repo-name>/...`. Filter flags: `--all-states`, `--test-states ak,wy`. Defaults to a 5-state sample (`al,ak,de,wy,sd`) when neither is set.

2. **`apply.py`** — orchestrator. Key sections:
   - `get_expected_repos` (lines 59–161): shells out to `render.py`, walks `generated/`, builds the set of repos that *should* exist.
   - `get_actual_repos` (lines 164–187): `gh repo list <org>`.
   - `create_repo` / `update_repo` / `delete_repo` (lines 190–484): reconcile.
   - `fully_override_dirs` (default `[".github"]`): which dirs in the target repo get authoritative overwrite — files there that aren't in the template get deleted. Other dirs are additive-merge, preserving user/data files.

3. **`config.schema.json`** — JSON Schema validating both YAMLs. Read this before adding a new locale knob.

## 4. How to make the common changes

### A. Add a new state/territory

1. Add a `locales.<code>:` entry to **both** `chn-openstates-scrape.yml` and `chn-openstates-files.yml`. Crib from a neighbor.
2. Add the matching entry to `/Users/sartaj/Git/govbot/actions/govbot/data/registry.json` under `us-legislation/<code>`. **This step is the easy one to forget — see §6.**
3. Run `./render-snapshots.sh` only if the new code is in the snapshot sample (`ak,id,mt,pr,wy`). Otherwise no snapshot churn.
4. Verify: `python3 apply.py -c chn-openstates-files.yml --test-states <code> --dry-run`.

### B. Mark a stuck jurisdiction as working / not-working (issue-#33 shape)

1. Add or remove `labels: [working]` on the locale entry in `chn-openstates-files.yml`.
2. The fix doesn't live here — diagnose the underlying scraper/formatter failure by inspecting the per-state repo's **Actions tab on GitHub** (e.g. `https://github.com/chn-openstates-files/az-legislation/actions`). This directory has zero runtime logs.
3. No snapshot change needed — `labels` is metadata, doesn't flow into rendered workflows.

### C. Change the workflow template (affects every jurisdiction)

1. Edit `templates/openstates-to-ocd-files/.github/workflows/format.yml` (files side) or `templates/openstates-scrape/...` (scrape side).
2. Run `./render-snapshots.sh` and commit the snapshot diff. **The diff in `__snapshots__/` is the review surface** — without it, reviewers can't see what 55 repos are about to receive.
3. Then `python3 apply.py -c <config>.yml --all-states --dry-run` to see how many repos would receive the update.

### D. Add a new dataset family that isn't OpenStates (Councilmatic — #30, Executive Actions — #28)

1. New template dir: `templates/<family>/` containing the workflow YAML the per-locale repos should carry.
2. New top-level config YAML next to the existing two, registering `template_markers`, `org`, `templates`, `locales`.
3. Wire the `templates:` block + `folder-name:` pattern. `apply.py` is family-agnostic — no Python changes needed.
4. Add resulting dataset IDs to `actions/govbot/data/registry.json`. If the namespace isn't `us-legislation`, set the right one (e.g. `us-executive`, `chicago-council`).

## 5. Verification loop

Always before any `gh repo create / update / delete` run:

```bash
cd actions/pipeline-manager

# 1. Render only — never hits GitHub:
python3 render.py -c chn-openstates-files.yml --test-states <code>

# 2. Reconcile preview — calls `gh repo list` but no mutations:
python3 apply.py -c chn-openstates-files.yml --test-states <code> --dry-run

# 3. Snapshot regen (only if a sample-set state changed OR a template changed):
./render-snapshots.sh
```

**Footgun:** the `to delete: N` line in the dry-run summary. If N is unexpectedly large, do NOT run without `--no-delete`. Some repos in the org may exist intentionally outside the catalog (e.g. issue-#32's proposed per-session repos would land that way).

## 6. The cross-tool sync gotcha — call this out loudly

The **Python catalog and the Rust registry are independent sources of truth** and they drift.

- **Python side** (`chn-openstates-{scrape,files}.yml`) sets `org.username: chn-openstates-files`. This is the org repos get created in.
- **Rust side** (`/Users/sartaj/Git/govbot/actions/govbot/data/registry.json`) is hand-maintained, baked into the binary via `include_str!`, and as of 2026-05 still points every `git_url` at `chn-openstates-files/` — the **predecessor** of the `govbot-data` org. Issue chihacknight/govbot#32 flags this.

If/when the `chn-openstates-files` → `govbot-data` org rename completes, **both** must move together. Touching only one creates a "user follows AGENT.md, lands on stale org" failure.

Same drift risk on every add/remove: Python adds the workflow repo, Rust needs the registry entry pointing at where data actually lands.

**Rule:** never change one without checking the other. One grep is enough:

```bash
rg "chn-openstates-files|govbot-data" actions/
```

## 7. Where this layer stops

What this directory does **not** own (don't drift into these):

- The actual scraper code — lives in the generated per-state repos + the upstream `openstates/openstates-scrapers` project.
- The OCD format conversion — lives in `actions/format/` and is invoked from the generated `format.yml` workflow.
- Text extraction (issue #31) — would be a new workflow step calling something under `actions/extract/`; this directory would only add the workflow-template wiring.
- The `govbot pull` cache, stream protocol, and `--select docs` projection — owned by `actions/govbot/` (Rust).

## Critical files

Read these first when in doubt:

- `chn-openstates-files.yml` — the catalog
- `chn-openstates-scrape.yml` — the scrape-side catalog
- `apply.py` (lines 59–161, 307–484) — orchestration + update logic
- `render.py` — template rendering
- `config.schema.json` — locale schema
- `render-snapshots.sh` — the 5-state sample, deterministic across platforms
- `templates/openstates-to-ocd-files/.github/workflows/format.yml` — the workflow template every files-side jurisdiction receives
- `/Users/sartaj/Git/govbot/actions/govbot/data/registry.json` — the Rust-side sync target (§6)
