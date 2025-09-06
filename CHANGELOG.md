# Changelog (Private Fork)

All notable changes in this private fork since adopting it have been documented below. This file summarizes functional updates, fixes, and developer‑facing notes to help future troubleshooting and restarts.

## Unreleased (work in progress)

- Weekly Awards: new section grouping award subsections under a single heading.
  - Best FA Pickup / Worst FA Pickup:
    - Sources adds/claims from Fleaflicker per‑team transactions and league activity, attributed to the correct fantasy week via Tuesday→Tuesday windows (derived from scoreboard).
    - Excludes players acquired via trade in the same week.
    - “Honorable mention” only appears when the benched candidate actually beats the started winner (Best: HM points > winner; Worst: HM points < winner).
    - Improved player name resolution (falls back across current/previous week rosters, current FA cache, and finally any week’s players_by_week) to avoid raw IDs.
  - Best Drop / Worst Drop:
    - Uses the same week attribution; points reflect that week’s scoring even if the player is a FA.
  - Weekly Best/Worst Trade (Net Points):
    - Computes from normalized Fleaflicker trades; excludes pick‑only sides (must have ≥1 pro player on both sides).

- Season‑to‑Date Trade Leader (Most Lopsided Trade):
  - Normalization adds `trade_id` and `trade_ts` to weekly trade entries.
  - Excludes pick‑only sides.
  - Carry‑forward aggregation: each qualifying trade contributes weekly net from its execution week through the report week (cumulative season‑to‑date).
  - Detail lists only players from the contributing `trade_id`(s) and includes the ids in the line (for cross‑checking on Fleaflicker).
  - Robust week mapping:
    - Tuesday→Tuesday windows from the scoreboard.
    - `trade_ts` (or requestedGames start) → window mapping is the source of truth; overrides any mismatched ordinals.
    - Offseason trades (pre‑Week 1) are attributed to Week 1 to begin carry‑forward.

- Best of the Rest (feature):
  - Builds an optimal starting lineup from free agents per week and compares vs each team.
  - Caching: reuses `output/data/<season>/<platform>/<league>/week_<n>/free_agents.json` for previous weeks (no re‑query), regardless of offline setting.
  - PDF adds:
    - “Best of the Rest” lineup with points and weekly mock record.
    - Optional season summary table and per‑team season records vs BOR.

- PDF formatting improvements:
  - All table cells use Paragraph with word wrap; vertical alignment = middle; prevents overflow/clipping.
  - Weekly Awards reflowed into titled subsections; award tables use appropriate widths and wrap.

- Week attribution (core):
  - Scoreboard‑derived Tuesday→Tuesday week windows (UTC epoch millis).
  - For items lacking ordinals, map by `timeEpochMilli` → window.
  - If ordinal disagrees with epoch window, use the window and log an override.

- Fleaflicker integrations and robustness:
  - Per‑team transactions: paginate far enough back to cover early‑season history; stop when crossing season start.
  - Activity and per‑team mapping now emit INFO logs for each mapped/skip item, plus a per‑week summary of adds/claims/drops/trades.
  - Trades: multi‑team trades supported by considering “players_received” for each team and unioning other teams’ received as that team’s “players_sent.”

- Logging & diagnostics:
  - Logs derived week windows for the season.
  - Logs per‑item mapping (“Activity map/skip…”, “TeamTx map/skip…”), per‑week transaction summaries, weekly award candidates (with samples), and trade carry‑forward contributions per week.

## Earlier (private fork baseline)

- Update checks disabled by default (`check_for_updates=False`).
- Footer customization via `FOOTER_TEXT`. Donation footer can be toggled with `SHOW_DONATION_FOOTER`.
- General resilience and formatting tweaks (see RELEASE.md: Private Fork Migration Notes).

---

Developer note: If you need to reproduce week mapping issues, enable `LOG_LEVEL=debug` (or `info`) and inspect the following:
- “Fleaflicker week windows for season …”
- “TeamTx map/skip …” and “Activity map/skip …” lines
- “Tx summary week N: adds=… claims=… drops=… trades=…”
- “Week N awards candidates …” with FA/Drops samples
- “Trade Szn add/skip … trade=<id> week=<wk> net=…” carry‑forward contributions
