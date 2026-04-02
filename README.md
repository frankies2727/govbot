[![Validate Snapshots](https://github.com/chihacknight/govbot/actions/workflows/validate-snapshots.yml/badge.svg)](https://github.com/chihacknight/govbot/actions/workflows/validate-snapshots.yml)
[![IL Witness Slip Notifications](https://github.com/chihacknight/govbot/actions/workflows/witness-slip-notifications.yml/badge.svg?branch=il-witness-slip-poc)](https://github.com/chihacknight/govbot/actions/workflows/witness-slip-notifications.yml)

**Project overview and demo**  
[![Govbot presentation video](https://img.youtube.com/vi/IFnE1oeUIXo/maxresdefault.jpg)](https://youtu.be/IFnE1oeUIXo)

# 🏛️ govbot

`govbot` enables distributed data analysis of government updates via a friendly terminal interface. Git repos function as datasets, including the legislation of all 47 states/jurisdictions.

## Quick Start

### 1. Install

```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/chihacknight/govbot/main/actions/govbot/scripts/install-nightly.sh)"
```

### 2. Set up your project

```bash
govbot
```

Running `govbot` with no config file launches an interactive setup wizard that:
1. Asks what data sources you want (all 47 states or specific ones)
2. Guides you through creating tags for topics you care about
3. Creates `govbot.yml`, `.gitignore`, and a GitHub Actions workflow

### 3. Run the pipeline

```bash
govbot
```

With a `govbot.yml` in your directory, running `govbot` executes the full pipeline:
1. Clones/updates legislation repositories
2. Tags bills based on your tag definitions
3. Generates RSS feeds in the `docs/` directory

### Other Commands

```bash
govbot clone all           # download all state legislation datasets
govbot clone il ca ny      # download specific states
govbot logs                # stream legislative activity as JSON Lines
govbot logs | govbot tag   # process and tag data
govbot build               # generate RSS feeds
govbot load                # load bill metadata into DuckDB
govbot delete all          # remove all downloaded data
govbot update              # update govbot to latest version
govbot --help              # see all commands and options
```

---

# 🚲🚇🏘️ IL Urbanist Witness Slip Notifier

> **Branch:** `il-witness-slip-poc`

An automated pipeline that tracks Illinois legislation relevant to urbanist causes — Housing, Biking, Safe Streets, and Transit — and alerts activist organizers when witness slips open for committee hearings.

## What It Does

Every weekday at 9 AM CT (and on every manual trigger), the pipeline:

1. **Parses ~8,600 IL bills** from the OpenStates dataset cloned by govbot
2. **Matches urbanist topics** — Housing, Biking, Safe Streets, Transit — using keyword tagging
3. **Always includes [Strong Towns Chicago](https://www.strongtownschicago.org/witness-slips) tracked bills** as a curated safety net, regardless of keyword matching
4. **Scrapes `ilga.gov/House/Schedules/Legislation`** and the Senate equivalent for bills with committee hearings scheduled in the current session
5. **Cross-references** tracked bills against the hearing schedule — any match moves from the watchlist into an urgent "Action Needed" banner
6. **Optionally verifies** that a witness slip form is actually live on ILGA (`check_slip_live: true` on manual runs)
7. **Generates a digest email** with two sections:
   - 🔔 **Action Needed** — bills with a hearing scheduled (prominent, with date/time)
   - 👀 **Watchlist** — all other tracked bills (collapsible, so the email stays clean when nothing is urgent)

## Email Structure

When a tracked bill reaches a committee hearing:

```
🔔 Action needed: the bills below have committee hearings scheduled.

Housing
  ✅ HB5626 (Proponent) — BUILD Act (Omnibus)
     🗓 Hearing: Apr 14, 2026 2:00 PM
     📝 File Witness Slip → ilga.gov/legislation/BillStatus.asp?...

👀 Watchlist — no hearing scheduled (18 bills) [click to expand]
  Housing (10 bills) ...
  Biking (3 bills) ...
```

When no hearings are scheduled, only the collapsible watchlist is shown — no noise.

## Tracked Bill Categories

| Category | What's tracked |
|---|---|
| **Housing** | BUILD Act package (ADUs, missing middle, parking reform, stair reform), YIGBY faith-based housing, adaptive reuse, AHPAA |
| **Biking** | Bike grid legislation, Idaho Stop, e-bike rules, bicycles-as-roadway-users |
| **Safe Streets** | Speed limit reductions, speed cameras, DUI threshold, Quick Build infrastructure |
| **Transit** | Green Light for Buses (transit signal priority) |

The tracked bill list is in `STC_TRACKED_BILLS` in `scripts/witness_slip_notifier.py` and is easy to extend.

## Setup

### GitHub Actions (recommended)

The workflow runs automatically. To enable email sending, add these secrets to your repo under **Settings → Secrets → Actions**:

| Secret | Description |
|---|---|
| `MAIL_SERVER` | SMTP server hostname (e.g. `smtp.gmail.com`) |
| `MAIL_PORT` | SMTP port (usually `465` for SSL) |
| `MAIL_USERNAME` | SMTP login username |
| `MAIL_PASSWORD` | SMTP password or app password |
| `MAIL_FROM` | From address (e.g. `govbot@yourdomain.org`) |
| `NOTIFICATION_RECIPIENTS` | Comma-separated list of recipient emails |
| `WITNESS_SLIP_USER_NAME` | Your name (pre-fills witness slip forms) |
| `WITNESS_SLIP_USER_ORG` | Your organization name |

Until `MAIL_SERVER` is set, the workflow runs in dry-run mode — it produces and uploads the full email digest as a build artifact (JSON, HTML, plain-text) every run so you can review it without sending.

### Manual Trigger

In the **Actions** tab (or via the VS Code GitHub Actions extension), trigger **IL Witness Slip Notifications** with these optional inputs:

| Input | Default | Effect |
|---|---|---|
| `test_mode` | `false` | Set `true` to skip email sending even if MAIL_SERVER is configured |
| `check_slip_live` | `false` | Set `true` to verify each bill's ILGA page for a live slip form before including it |

### Local Testing

```bash
# Install dependencies
pip install requests

# Run against locally cloned OpenStates data
python3 scripts/witness_slip_notifier.py \
  --mode local \
  --data-dir .govbot/repos/il-legislation/country:us/state:il/sessions/104th/bills

# Or download a small sample automatically
python3 scripts/witness_slip_notifier.py --sample
```

## How the Pipeline Filters Bills

```
~8,648 IL bills (OpenStates, 104th General Assembly)
    ↓  topic match: Housing / Biking / Safe Streets / Transit
~20 urbanist-tagged bills  +  STC curated list (always included)
    ↓  ILGA hearing schedule scrape (live, every run)
bills with hearings on the calendar  →  🔔 Action Needed section
    ↓  optional: live slip check (CHECK_SLIP_LIVE=true)
bills where slip form is confirmed open  →  sent immediately
all other tracked bills  →  👀 Watchlist (collapsible)
```

The ILGA hearing scrape hits `ilga.gov/House/Schedules/Legislation` and `ilga.gov/Senate/Schedules/Legislation` directly, matching bill numbers to scheduled dates. No manual monitoring needed — if a bill gets a hearing, the next daily run will catch it and move it into the urgent section automatically.

## Files

| File | Purpose |
|---|---|
| `scripts/witness_slip_notifier.py` | Main pipeline script |
| `.github/workflows/witness-slip-notifications.yml` | GitHub Actions workflow |

---

# 🏛️ Govbot Legislation Effort

- Nearly all state governments
- Federal

WIP: Ideally, these scripts should be accessible via the following ways.

- CLI / Unix pipe friendliness where possible. CLI is the most portable of solutions.
- GitHub Actionable if possible

## Contribute

### Folder Structure

This repo is a monorepo, with `actions` being self contained. `actions` as a name is because it's what Github expects.

### Requirements For Each Action

- Be a runnable as basic scripts in python, bash, rust, or typescript which can run as shell scripts with args.
- Have an `action.yml` file to run as a runner, most likely in GitHub Actions.
- Have a `schemas` folder that uses JSON schema to define types.
  - This allow other actions to import your schema for validation.
- Have `__snapshots__` that contain real file/folder outputs. This serves two purposes: (1) they show expected results and (2) they can be directly used as inputs for downstream snapshot tests.
  - Each action manages its own snapshot rendering through a render_snapshots.sh script.
  - Validation occurs via .github/validate-snapshots.yml for each specific module.
