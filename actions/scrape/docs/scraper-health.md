# Scraper Health Log

Tracks the status of all 56 `govbot-openstates-scrapers` repos. Updated manually after health checks.

---

## 2026-07-02 Status Update

### Self-Hosted Runner States

3 runners now active on MacBook (`~/actions-runner/`, `~/actions-runner-2/`, `~/actions-runner-3/`) registered at org level (`govbot-openstates-scrapers`).

| State | Status | Notes |
|-------|--------|-------|
| tx | ⏸️ out of session | `capitol.texas.gov` blocks Azure IPs. Runner ready. Resumes ~Jan 2027. |
| ma | ✅ running daily | First self-hosted run completed 2026-07-02 in 7h 43m. PR [#53](https://github.com/chihacknight/govbot/pull/53) ✅ merged. Running daily going forward. |
| fl | 🔄 backfill running | End-of-session capture in progress on self-hosted runner. PRs [#53](https://github.com/chihacknight/govbot/pull/53) + [#55](https://github.com/chihacknight/govbot/pull/55) ✅ merged. |
| tn | 🔄 backfill running | 114th GA ended ~2026-04-25. Only 37/5,400+ bills captured (IP block). Self-hosted runner added via PR [#56](https://github.com/chihacknight/govbot/pull/56) ✅ merged. Apply-templates done. Full backfill running now. |

### Fix Pending

| State | Status | Notes |
|-------|--------|-------|
| va | 🔧 waiting on OpenStates | `scrape.sh` arg order fixed + session bumped to 2026 via PR [#54](https://github.com/chihacknight/govbot/pull/54) ✅ merged + apply-templates done. Waiting on OpenStates PR [#5717](https://github.com/openstates/openstates-scrapers/pull/5717) to merge before running. |
| wv | 🔧 waiting on OpenStates | XPath broken after site redesign — 0 bills scraped. PR [#5719](https://github.com/openstates/openstates-scrapers/pull/5719) 🔄 open. Backfill after merge. |
| vi | ❌ server down | `billtracking.legvi.org:8082` offline. Active session but no code fix possible. |

### Backfill Needed (Docker Timing / Stale Cache)

These states have accessible APIs but were scraped with partial data due to Docker image timing or stale GitHub Actions cache.

| State | Files | Notes |
|-------|------:|-------|
| sd | 41 | Cache cleared 2026-07-02. Fresh backfill dispatch running now — expect 666 bills. |
| ut | 28 | 3/1,016 2026 bills (stale cache) + 5 complete 2025S2 bills. Needs cache clear + dispatch. |
| in | 47 | ~40/1,000+ bills. Docker got 2026 session Mar 23; session ended Feb 27. Needs dispatch. |
| id | 5 | 1 bill only. Docker got 2026 session late; session ended Apr 2. Needs dispatch. |

### Needs Verification Run

| State | Notes |
|-------|-------|
| ok | PR [#5718](https://github.com/openstates/openstates-scrapers/pull/5718) ✅ merged 2026-07-01 — (PROD) suffix fix. Trigger manual dispatch to confirm bills scraped. |

### WAF / Site Blocking (OpenStates Fix Needed)

| State | Files | Issue | Notes |
|-------|------:|-------|-------|
| az | 4 | [#1382](https://github.com/openstates/issues/issues/1382) 🔄 | Sucuri WAF blocks `setsession.php` POST. Full 2026 session missed. |
| hi | 4 | [#1383](https://github.com/openstates/issues/issues/1383) 🔄 | Cloudflare WAF blocks all bill pages. Full 2026 session missed. |

### FTP Data Sources (OpenStates Fix Needed)

| State | Files | Issue | Notes |
|-------|------:|-------|-------|
| nm | 4 | [#1381](https://github.com/openstates/issues/issues/1381) 🔄 | FTP-only data; scrapelib can't handle `ftp://`. |
| ct | 4 | [#1384](https://github.com/openstates/issues/issues/1384) 🔄 | 5 FTP endpoints; scrapelib can't handle `ftp://`. More complex fix. |

### Active Session / Scraper Mystery

| State | Files | Notes |
|-------|------:|-------|
| ar | 4 | Active session `2026S1` (May 4–Aug 15). FTP source has 2 bills (SB1, HB1001) but scraper produces 0 with EXIT_CODE=0. Root cause unclear — likely stale scrapelib cache. Needs Docker-level investigation. |
| la | 7 | Crash fixed PR [#5716](https://github.com/openstates/openstates-scrapers/pull/5716) ✅. Only 7/525 bills returned — bill search pattern issues. Issue [#1379](https://github.com/openstates/issues/issues/1379) 🔄 open, waiting on maintainers. |

### Clean / Expected Low

All other states running successfully. States with low counts are expected (short sessions, budget-only, or no 2026 session).

| State | Files | Notes |
|-------|------:|-------|
| ny | 24,937 | |
| ca | 22,680 | |
| il | 15,323 | |
| mt | 13,258 | |
| usa | 11,960 | |
| mn | 9,815 | |
| nj | 8,212 | |
| pa | 6,423 | |
| wa | 5,713 | |
| pr | 5,081 | |
| sc | 3,947 | |
| ri | 4,132 | |
| mo | 4,640 | |
| ia | 3,748 | |
| me | 3,613 | |
| nd | 3,265 | |
| ms | 3,033 | |
| mi | 2,913 | |
| dc | 2,713 | PRs #5706, #5711 confirmed working |
| wi | 2,701 | |
| nc | 2,432 | |
| oh | 2,167 | |
| ne | 1,976 | |
| al | 1,511 | |
| ks | 1,487 | |
| ak | 942 | |
| vt | 953 | |
| gu | 833 | |
| co | 736 | |
| ga | 460 | |
| md | 535 | |
| nh | 437 | |
| or | 268 | |
| mp | 139 | |
| ky | 78 | 30-day even-year session |
| nv | 64 | No 2026 session (2025S data) |
| wy | 27 | Budget session only |

---

## Known Ongoing Issues

### TN — IP Block by wapp.capitol.tn.gov

TN's 114th General Assembly (2025-2026) ended ~2026-04-25. The scraper was blocked by `wapp.capitol.tn.gov` early in the run — only 37 of an estimated ~5,400+ bills were captured before the block hit. Site is accessible from non-cloud IPs; block is specific to GitHub-hosted runner IPs (N1_ACTIVE_BLOCK), same pattern as TX.

**Bill count estimate**: Index at `wapp.capitol.tn.gov/apps/indexes/BillsByIndex/?year=114` shows 98 listing pages — HB0001–HB2671, SB0001–SB2733, plus HJR, SJR, HR, SR series. Roughly 5,400+ total.

**Fix applied 2026-07-02**: `runner: self-hosted` added to TN (PR [#56](https://github.com/chihacknight/govbot/pull/56)), apply-templates run, full backfill dispatch triggered. Next session: 115th GA ~January 2027.

### MA — Active Throttling by malegislature.gov

`malegislature.gov` throttles Azure-originating requests progressively: 36s → 72s → 300s → connection drop. Fixed by moving to self-hosted runner. First successful run 2026-07-02 completed in 7h 43m. Now runs daily on MacBook runner.

### FL — Serial Per-Bill Scraping

FL scraper fetches each bill individually (BillDetail + HouseSearchPage + N vote PDFs). One bill (HJR 1F) took ~34s with 7 vote PDF fetches. Across two sessions (`2026` + `2026F`), this exceeds the 6-hour GitHub Actions cap. No IP blocking — it's just slow. Fixed by moving to self-hosted runner (no time cap). End-of-session backfill running 2026-07-02.

### VA — Scrape Argument Bug (Waiting on OpenStates)

`scrape.sh` arg order fixed and session bumped to 2026 (PR [#54](https://github.com/chihacknight/govbot/pull/54) ✅). Apply-templates run. Waiting on OpenStates PR [#5717](https://github.com/openstates/openstates-scrapers/pull/5717) to fix hardcoded `session_id="20251"` in `csv_bills`. Will trigger scrape once #5717 merges.

### TX — Self-Hosted Runner Dependency (Out of Session)

TX out of session (no 2026 regular session). Requires MacBook runner when active — `capitol.texas.gov` blocks Azure IPs. Runner registered at org level. Runbook: `actions/scrape/docs/tx-backfill-runbook.md`.

---

## govbot PRs

| PR | Description | Status |
|----|-------------|--------|
| [#52](https://github.com/chihacknight/govbot/pull/52) | VA/VI scraper arg order + session fix | ✅ Merged |
| [#53](https://github.com/chihacknight/govbot/pull/53) | MA + FL self-hosted runner | ✅ Merged 2026-07-02 |
| [#54](https://github.com/chihacknight/govbot/pull/54) | VA scrape.sh arg order + session 2026 | ✅ Merged 2026-07-02 |
| [#55](https://github.com/chihacknight/govbot/pull/55) | MA/FL runner docs + scrape.sh grep -E fix | ✅ Merged 2026-07-02 |
| [#56](https://github.com/chihacknight/govbot/pull/56) | TN self-hosted runner + IP block docs | ✅ Merged 2026-07-02 |

## OpenStates PRs Filed by Tamara (tamara-builds)

| PR | Description | Status |
|----|-------------|--------|
| [#5706](https://github.com/openstates/openstates-scrapers/pull/5706) | DC: scraper crashes on non-PDF attachments | ✅ Merged 2026-06-29 |
| [#5707](https://github.com/openstates/openstates-scrapers/pull/5707) | NJ: skip votes for bills missing from bill_dict | ✅ Merged 2026-06-29 |
| [#5711](https://github.com/openstates/openstates-scrapers/pull/5711) | DC: handle PDF URLs with query strings | ✅ Merged 2026-06-30 |
| [#5712](https://github.com/openstates/openstates-scrapers/pull/5712) | Biennium end_date off-by-one (DC, MI, NC, PA) | ✅ Merged 2026-07-01 |
| [#5716](https://github.com/openstates/openstates-scrapers/pull/5716) | LA: handle variable action table column count | ✅ Merged 2026-07-01 |
| [#5717](https://github.com/openstates/openstates-scrapers/pull/5717) | VA: fix csv_bills hardcoded session ID | 🔄 Open |
| [#5718](https://github.com/openstates/openstates-scrapers/pull/5718) | OK: strip (PROD) suffix from session list | ✅ Merged 2026-07-01 |
| [#5719](https://github.com/openstates/openstates-scrapers/pull/5719) | WV: XPath broken after site redesign | 🔄 Open |

## OpenStates Issues Filed by Tamara

| Issue | Description | Status |
|-------|-------------|--------|
| [#1372](https://github.com/openstates/issues/issues/1372) | Filed 2026-06 | 🔄 Open |
| [#1373](https://github.com/openstates/issues/issues/1373) | Filed 2026-06 | 🔄 Open |
| [#1374](https://github.com/openstates/issues/issues/1374) | Filed 2026-06 | 🔄 Open |
| [#1375](https://github.com/openstates/issues/issues/1375) | Filed 2026-06 | 🔄 Open |
| [#1376](https://github.com/openstates/issues/issues/1376) | LA: action table column count varies | ✅ Closed (PR #5716) |
| [#1377](https://github.com/openstates/issues/issues/1377) | VA: csv_bills hardcoded session ID | 🔄 Open (PR #5717 pending) |
| [#1378](https://github.com/openstates/issues/issues/1378) | OK: (PROD) suffix not stripped | ✅ Closed (PR #5718) |
| [#1379](https://github.com/openstates/issues/issues/1379) | LA: bill search returning ~7 of 525 bills | 🔄 Open — waiting on maintainers |
| [#1380](https://github.com/openstates/issues/issues/1380) | WV: XPath broken after site redesign | 🔄 Open (PR #5719 pending) |
| [#1381](https://github.com/openstates/issues/issues/1381) | NM: FTP-only data, scrapelib can't handle ftp:// | 🔄 Open |
| [#1382](https://github.com/openstates/issues/issues/1382) | AZ: Sucuri WAF blocks setsession.php POST | 🔄 Open |
| [#1383](https://github.com/openstates/issues/issues/1383) | HI: Cloudflare WAF blocks bill pages | 🔄 Open |
| [#1384](https://github.com/openstates/issues/issues/1384) | CT: 5 FTP endpoints, scrapelib can't handle ftp:// | 🔄 Open |
