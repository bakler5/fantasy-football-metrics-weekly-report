__author__ = "Josh Bachler (fork maintainer); original: Wren J. R. (uberfastman)"
__email__ = "bakler5@gmail.com"

import itertools
from typing import List

from ffmwr.calculate.metrics import CalculateMetrics
from ffmwr.calculate.points_by_position import PointsByPosition
from ffmwr.calculate.coaching_efficiency import CoachingEfficiency
from ffmwr.models.base.model import BaseLeague, BaseMatchup, BaseTeam
from ffmwr.utilities.app import add_report_team_stats, get_inactive_players
from ffmwr.utilities.logger import get_logger
from ffmwr.utilities.settings import AppSettings

logger = get_logger(__name__, propagate=False)


class ReportData(object):
    def __init__(
        self,
        settings: AppSettings,
        league: BaseLeague,
        season_weekly_teams_results,
        week_counter: int,
        week_for_report: int,
        season: int,
        metrics_calculator: CalculateMetrics,
        metrics,
        break_ties: bool = False,
        dq_ce: bool = False,
        testing: bool = False,
        show_optimal_lineup: bool = False,
    ):
        logger.debug("Instantiating report data.")

        self.league: BaseLeague = league
        self.break_ties: bool = break_ties
        self.dq_ce: bool = dq_ce
        self.week: int = league.week
        self.bench_positions: List[str] = league.bench_positions
        self.has_divisions: bool = league.has_divisions
        self.has_waiver_priorities: bool = league.has_waiver_priorities
        self.is_faab: bool = league.is_faab

        inactive_players = []
        if dq_ce:
            inactive_players = get_inactive_players(week_counter, league)

        self.teams_results = {
            team.team_id: add_report_team_stats(
                settings,
                team,
                league,
                week_counter,
                metrics_calculator,
                metrics,
                dq_ce,
                inactive_players,
                show_optimal_lineup,
            )
            for team in league.teams_by_week.get(str(week_counter)).values()
        }

        records = {}
        for team_id, team in self.teams_results.items():
            records[team_id] = team.record

        league.standings = sorted(
            league.teams_by_week.get(str(week_counter)).values(),
            key=lambda x: (
                league.records_by_week[str(week_counter)][x.team_id].rank,
                -league.records_by_week[str(week_counter)][x.team_id].get_points_for(),
            ),
        )

        # option to disqualify team(s) manually entered in the .env file for current week of coaching efficiency
        self.coaching_efficiency_dqs = {}
        if week_counter == week_for_report:
            for team in settings.coaching_efficiency_disqualified_teams_list:
                self.coaching_efficiency_dqs[team] = -2
                for team_result in self.teams_results.values():
                    if team == team_result.name:
                        team_result.coaching_efficiency = "DQ"

        # used only for testing what happens when different metrics are tied; requires uncommenting lines in method
        if testing:
            metrics_calculator.test_ties(self.teams_results)

        # get remaining matchups for Monte Carlo playoff simulations
        remaining_matchups = {}
        for week, matchups in league.matchups_by_week.items():
            if int(week) > week_for_report:
                remaining_matchups[str(week)] = []
                matchup: BaseMatchup
                for matchup in matchups:
                    matchup_teams = []
                    for team in matchup.teams:
                        matchup_teams.append(team.team_id)
                    remaining_matchups[str(week)].append(tuple(matchup_teams))

        # calculate z-scores (dependent on all previous weeks scores)
        z_score_results = metrics_calculator.calculate_z_scores(season_weekly_teams_results + [self.teams_results])

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ REPORT DATA ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        logger.debug("Creating report data.")

        # create attributes for later updating
        self.data_for_season_avg_points_by_position = None
        self.data_for_season_weekly_top_scorers = None
        self.data_for_season_weekly_low_scorers = None
        self.data_for_season_weekly_highest_ce = None
        # Transactions & lineup awards (initial implementation focuses on worst start/sit)
        self.transactions_awards_worst_startsit = []
        self.transactions_awards_best_fa_pickups = []  # single row
        self.transactions_awards_worst_fa_pickups = []  # single row
        self.transactions_awards_best_fa_pickups_hm = None
        self.transactions_awards_worst_fa_pickups_hm = None
        self.transactions_awards_best_drops = []  # single row
        self.transactions_awards_worst_drops = []  # single row
        self.transactions_awards_best_trades = []  # single row
        self.transactions_awards_worst_trades = []  # single row
        # Best of the Rest (optional)
        self.best_of_rest_lineup = None
        self.best_of_rest_total = None
        self.best_of_rest_week_results = None
        self.best_of_rest_week_record = None
        self.best_of_rest_season_record = None
        # season-to-date trade leader (set in builder)
        self.transactions_awards_best_trade_season = []

        # current standings data
        self.data_for_current_standings = metrics_calculator.get_standings_data(league)

        # current division standings data
        self.divisions = None
        self.data_for_current_division_standings = None
        if self.has_divisions:
            self.divisions = league.divisions
            self.data_for_current_division_standings = metrics_calculator.get_division_standings_data(league)

        # current median standings data
        self.data_for_current_median_standings = metrics_calculator.get_median_standings_data(league)
        # expose week median for the report week (if available from platform mapping)
        try:
            self.week_median_for_report = league.median_score_by_week.get(str(week_for_report))
        except Exception:
            self.week_median_for_report = None

        if league.num_playoff_slots > 0:
            # playoff probabilities data
            self.data_for_playoff_probs = metrics.get("playoff_probs").calculate(
                week_counter, week_for_report, league.standings, remaining_matchups
            )
        else:
            self.data_for_playoff_probs = None

        if self.data_for_playoff_probs:
            self.data_for_playoff_probs = metrics_calculator.get_playoff_probs_data(
                league.standings, self.data_for_playoff_probs
            )
        else:
            self.data_for_playoff_probs = None

        # Best of the Rest for this week (if free agents available)
        try:
            free_agents = league.free_agents_by_week.get(str(week_counter), {})
            if free_agents and settings.report_settings.best_of_rest_bool:
                logger.info(
                    f"Best of the Rest: week {week_counter} has {len(free_agents)} raw free agents prior to filtering."
                )
                # filter out any players who are rostered this week
                rostered_ids = set(league.players_by_week.get(str(week_counter), {}).keys())
                filtered_fas = [p for pid, p in free_agents.items() if str(pid) not in rostered_ids]
                logger.info(
                    f"Best of the Rest: week {week_counter} after filtering rostered -> {len(filtered_fas)} free agents."
                )
                if not filtered_fas:
                    raise ValueError("No eligible free agents after filtering out rostered players.")

                ce = CoachingEfficiency(league)
                optimal_lineup, optimal_total = ce.compute_optimal_lineup_for_roster(filtered_fas)

                # lineup rows: Position, Player, NFL Team, Points
                lineup_rows = []
                for pos, slot in optimal_lineup.items():
                    for assigned in slot.assigned_players:
                        lineup_rows.append([pos, assigned.full_name, assigned.nfl_team_abbr, f"{assigned.points:.2f}"])

                # mock results vs each team
                week_results = []
                wins = ties = losses = 0
                for team in league.teams_by_week.get(str(week_counter)).values():
                    team_pts = float(team.points)
                    if optimal_total > team_pts:
                        result = "W"
                        wins += 1
                    elif optimal_total < team_pts:
                        result = "L"
                        losses += 1
                    else:
                        result = "T"
                        ties += 1
                    week_results.append([team.name, f"{team_pts:.2f}", f"{optimal_total:.2f}", result])

                self.best_of_rest_lineup = lineup_rows
                self.best_of_rest_total = f"{optimal_total:.2f}"
                self.best_of_rest_week_results = week_results
                self.best_of_rest_week_record = (wins, losses, ties)
                logger.info(
                    f"Best of the Rest: week {week_counter} optimal total {optimal_total:.2f}, record {wins}-{losses}"
                    f"{f'-{ties}' if ties else ''}."
                )
        except Exception as e:
            logger.debug(f"Best of the Rest unavailable for week {week_counter}: {e}")

        # z-scores data
        self.data_for_z_scores = []
        z_score_rank = 1
        if all(z_score_val is None for z_score_val in z_score_results.values()):
            create_z_score_data = False
        elif any(z_score_val is None for z_score_val in z_score_results.values()):
            create_z_score_data = True
            z_score_results = {
                team_id: 0 if not z_score_val else z_score_val for team_id, z_score_val in z_score_results.items()
            }
        else:
            create_z_score_data = True

        if create_z_score_data:
            for k_v in sorted(z_score_results.items(), key=lambda x: x[1], reverse=True):
                z_score = k_v[1]
                if z_score:
                    z_score = round(float(z_score), 2)
                else:
                    z_score = "N/A"

                team = self.teams_results[k_v[0]]
                self.data_for_z_scores.append([z_score_rank, team.name, team.manager_str, z_score])
                z_score_rank += 1

        # points by position data
        points_by_position = PointsByPosition(league, week_for_report)
        self.data_for_weekly_points_by_position = points_by_position.get_weekly_points_by_position(self.teams_results)

        # teams data and season average points by position data
        self.data_for_teams = []
        team_result: BaseTeam
        for team_result in self.teams_results.values():
            self.data_for_teams.append(
                [
                    team_result.team_id,
                    team_result.name,
                    team_result.manager_str,
                    team_result.points,
                    team_result.coaching_efficiency,
                    team_result.luck,
                    team_result.optimal_points,
                    z_score_results[team_result.team_id],
                ]
            )

        self.data_for_teams.sort(key=lambda x: x[1])

        # scores data
        self.data_for_scores = metrics_calculator.get_score_data(
            sorted(self.teams_results.values(), key=lambda x: float(x.points), reverse=True)
        )

        # coaching efficiency data
        self.data_for_coaching_efficiency = metrics_calculator.get_coaching_efficiency_data(
            sorted(
                self.teams_results.values(),
                key=lambda x: float(x.coaching_efficiency) if x.coaching_efficiency != "DQ" else 0,
                reverse=True,
            )
        )
        self.num_coaching_efficiency_dqs = metrics_calculator.coaching_efficiency_dq_count
        self.coaching_efficiency_dqs.update(metrics.get("coaching_efficiency").coaching_efficiency_dqs)

        # luck data
        self.data_for_luck = metrics_calculator.get_luck_data(
            sorted(self.teams_results.values(), key=lambda x: float(x.luck), reverse=True)
        )

        # optimal score data
        self.data_for_optimal_scores = metrics_calculator.get_optimal_score_data(
            sorted(self.teams_results.values(), key=lambda x: float(x.optimal_points), reverse=True)
        )

        # bad boy data
        self.data_for_bad_boy_rankings = metrics_calculator.get_bad_boy_data(
            sorted(self.teams_results.values(), key=lambda x: x.bad_boy_points, reverse=True)
        )

        # beef rank data
        self.data_for_beef_rankings = metrics_calculator.get_beef_rank_data(
            sorted(self.teams_results.values(), key=lambda x: x.tabbu, reverse=True)
        )

        # high roller data
        self.data_for_high_roller_rankings = metrics_calculator.get_high_roller_data(
            sorted(self.teams_results.values(), key=lambda x: x.fines_total, reverse=True)
        )

        # Transactions & Lineup Awards (compute for the current week)
        try:
            # Only evaluate on the actual report week (not historical aggregation loop)
            if int(week_counter) == int(week_for_report):
                bench_positions = set(self.bench_positions or [])
                awards_rows = []

                for team in self.league.teams_by_week.get(str(week_counter), {}).values():
                    # collect starters and bench with available points
                    starters = []
                    bench = []
                    for player in team.roster:
                        try:
                            pts = float(player.points)
                        except Exception:
                            pts = 0.0
                        sel_pos = player.selected_position
                        if not sel_pos:
                            continue
                        if sel_pos in bench_positions:
                            bench.append((player, pts))
                        else:
                            starters.append((player, pts))

                    # find the single worst start/sit opportunity for this team respecting eligibility
                    max_delta = 0.0
                    worst_b = None
                    worst_s = None
                    worst_b_pts = 0.0
                    worst_s_pts = 0.0
                    for b, b_pts in bench:
                        # eligible positions include base and flex entries populated during platform mapping
                        eligible_positions = b.eligible_positions or set()
                        for s, s_pts in starters:
                            if s.selected_position in eligible_positions:
                                delta = b_pts - s_pts
                                if delta > max_delta:
                                    max_delta = delta
                                    worst_b = b
                                    worst_s = s
                                    worst_b_pts = b_pts
                                    worst_s_pts = s_pts

                    if max_delta > 0.0 and worst_b and worst_s:
                        awards_rows.append(
                            [
                                team.name,
                                team.manager_str,
                                f"{worst_b.full_name} ({worst_b.primary_position}) {worst_b_pts:.2f}",
                                f"{worst_s.full_name} ({worst_s.selected_position}) {worst_s_pts:.2f}",
                                f"{max_delta:.2f}",
                            ]
                        )

                # select single highest delta across league
                awards_rows.sort(key=lambda r: float(r[-1]), reverse=True)
                self.transactions_awards_worst_startsit = awards_rows[:1]

                # Weekly FA pickups and drops using Fleaflicker activity
                curr_week = int(week_counter)
                curr_players = self.league.players_by_week.get(str(curr_week), {})
                prev_players = self.league.players_by_week.get(str(curr_week - 1), {}) if curr_week > self.league.start_week else {}
                curr_free_agents = self.league.free_agents_by_week.get(str(curr_week), {})
                events = self.league.transactions_by_week.get(
                    str(curr_week), {"adds": [], "claims": [], "drops": [], "trades": []}
                )

                # unify adds and claims as FA pickups
                pickups = list(events.get("adds", [])) + list(events.get("claims", []))
                fa_candidates = []  # (team_id, player_id, points)
                drop_candidates = []  # (team_id, player_id, points)

                # per-team roster lookups to check started/not
                curr_team_objs = self.league.teams_by_week.get(str(curr_week), {})
                team_rosters = {tid: {str(p.player_id): p for p in t.roster} for tid, t in curr_team_objs.items()}

                # Build trade-received set for exclusion from FA
                trade_received_by_team = {}
                for tr in events.get("trades", []):
                    ttid = str(tr.get("team_id"))
                    rec_ids = [str(x) for x in tr.get("players_received", [])]
                    trade_received_by_team.setdefault(ttid, set()).update(rec_ids)

                for ev in pickups:
                    tid = str(ev.get("team_id"))
                    pid_s = str(ev.get("player_id"))
                    # exclude players acquired via trade this week for that team
                    if pid_s in trade_received_by_team.get(tid, set()):
                        continue
                    bp = curr_players.get(pid_s) or curr_players.get(int(pid_s)) if hasattr(curr_players, 'get') else None
                    pts = float(bp.points) if bp else 0.0
                    fa_candidates.append((tid, pid_s, pts))

                for ev in events.get("drops", []):
                    tid = str(ev.get("team_id"))
                    pid_s = str(ev.get("player_id"))
                    # points for the player this week either on another roster or as FA
                    if curr_players.get(pid_s) or curr_players.get(int(pid_s)):
                        bp = curr_players.get(pid_s) or curr_players.get(int(pid_s))
                        pts = float(bp.points)
                    elif curr_free_agents.get(pid_s):
                        pts = float(curr_free_agents.get(pid_s).points)
                    else:
                        pts = 0.0
                    drop_candidates.append((tid, pid_s, pts))

                    # Map team_id -> (name, manager)
                    team_lookup = {tid: (t.name, t.manager_str) for tid, t in curr_team_objs.items()}

                    # IMPORTANT: Do NOT infer via roster diffs by default to avoid mis-dating historical changes
                    # If desired in the future, add a settings flag to enable a diff-based fallback.

                    # Helper to resolve a player's display name with broader fallbacks
                    def resolve_name(pid_str: str) -> str:
                        bp = (
                            curr_players.get(pid_str)
                            or (curr_players.get(int(pid_str)) if hasattr(curr_players, 'get') else None)
                            or prev_players.get(pid_str)
                            or (prev_players.get(int(pid_str)) if hasattr(prev_players, 'get') else None)
                            or curr_free_agents.get(pid_str)
                        )
                        if bp and getattr(bp, 'full_name', None):
                            return bp.full_name
                        try:
                            for wk_players in self.league.players_by_week.values():
                                hit = wk_players.get(pid_str) or wk_players.get(int(pid_str))
                                if hit and getattr(hit, 'full_name', None):
                                    return hit.full_name
                        except Exception:
                            pass
                        return str(pid_str)

                    # Split FA pickups by whether they were STARTED this week
                    started_fa = []   # (team_id, player_id, points)
                    benched_fa = []   # honorable mention candidates
                    bench_positions_set = bench_positions
                    for tid, pid, pts in fa_candidates:
                        troster = team_rosters.get(tid, {})
                        p = troster.get(str(pid))
                        if p and p.selected_position and p.selected_position not in bench_positions_set:
                            started_fa.append((tid, pid, pts))
                        else:
                            benched_fa.append((tid, pid, pts))

                    # Log detailed samples for troubleshooting
                    try:
                        sample_fa = ", ".join(
                            [
                                f"{team_lookup.get(tid, ('?', '?'))[0]}:{resolve_name(pid)}:{pts:.2f}"
                                for tid, pid, pts in fa_candidates[:5]
                            ]
                        )
                        sample_drop = ", ".join(
                            [
                                f"{team_lookup.get(tid, ('?', '?'))[0]}:{resolve_name(pid)}:{pts:.2f}"
                                for tid, pid, pts in drop_candidates[:5]
                            ]
                        )
                    except Exception:
                        sample_fa = sample_drop = ""

                    logger.info(
                        f"Week {curr_week} awards candidates -> pickups(total={len(fa_candidates)}, "
                        f"started={len(started_fa)}, drops={len(drop_candidates)}, trades={len(events.get('trades', []))}); "
                        f"FA sample: [{sample_fa}] Drops sample: [{sample_drop}]"
                    )

                    # Best FA Pickup (started only). If none started, fall back to best benched pickup.
                    if started_fa:
                        best_tid, best_pid, best_pts = sorted(started_fa, key=lambda x: x[2], reverse=True)[0]
                        if best_tid in team_lookup:
                            self.transactions_awards_best_fa_pickups = [
                                [team_lookup[best_tid][0], team_lookup[best_tid][1], resolve_name(str(best_pid)), f"{best_pts:.2f}"]
                            ]
                        # HM only if a benched pickup scored MORE than the started winner
                        self.transactions_awards_best_fa_pickups_hm = None
                        if benched_fa:
                            hm_tid, hm_pid, hm_pts = sorted(benched_fa, key=lambda x: x[2], reverse=True)[0]
                            if hm_tid in team_lookup and hm_pts > best_pts:
                                self.transactions_awards_best_fa_pickups_hm = (
                                    f"Honorable mention (not started): {team_lookup[hm_tid][0]} — {resolve_name(str(hm_pid))} ({hm_pts:.2f})"
                                )
                    elif benched_fa:
                        # fallback winner when no started FA pickups
                        best_tid, best_pid, best_pts = sorted(benched_fa, key=lambda x: x[2], reverse=True)[0]
                        if best_tid in team_lookup:
                            self.transactions_awards_best_fa_pickups = [
                                [team_lookup[best_tid][0], team_lookup[best_tid][1], resolve_name(str(best_pid)), f"{best_pts:.2f}"]
                            ]

                    # Worst FA Pickup (started only). If none started, fall back to worst benched pickup.
                    if started_fa:
                        worst_tid, worst_pid, worst_pts = sorted(started_fa, key=lambda x: x[2])[0]
                        if worst_tid in team_lookup:
                            self.transactions_awards_worst_fa_pickups = [
                                [team_lookup[worst_tid][0], team_lookup[worst_tid][1], resolve_name(str(worst_pid)), f"{worst_pts:.2f}"]
                            ]
                        # HM only if a benched pickup scored LESS than the started worst winner
                        self.transactions_awards_worst_fa_pickups_hm = None
                        if benched_fa:
                            hm_tid2, hm_pid2, hm_pts2 = sorted(benched_fa, key=lambda x: x[2])[0]
                            if hm_tid2 in team_lookup and hm_pts2 < worst_pts:
                                self.transactions_awards_worst_fa_pickups_hm = (
                                    f"Honorable mention (not started): {team_lookup[hm_tid2][0]} — {resolve_name(str(hm_pid2))} ({hm_pts2:.2f})"
                                )
                    elif benched_fa:
                        worst_tid, worst_pid, worst_pts = sorted(benched_fa, key=lambda x: x[2])[0]
                        if worst_tid in team_lookup:
                            self.transactions_awards_worst_fa_pickups = [
                                [team_lookup[worst_tid][0], team_lookup[worst_tid][1], resolve_name(str(worst_pid)), f"{worst_pts:.2f}"]
                            ]

                    # Build drop awards rows (best = lowest points by dropped player, worst = highest) — single winner
                    if drop_candidates:
                        drop_candidates.sort(key=lambda x: x[2])
                        tid, pid, pts = drop_candidates[0]
                        name = resolve_name(str(pid))
                        if tid in team_lookup:
                            self.transactions_awards_best_drops = [
                                [team_lookup[tid][0], team_lookup[tid][1], name, f"{pts:.2f}"]
                            ]
                        drop_candidates.sort(key=lambda x: x[2], reverse=True)
                        tid, pid, pts = drop_candidates[0]
                        name = resolve_name(str(pid))
                        if tid in team_lookup:
                            self.transactions_awards_worst_drops = [
                                [team_lookup[tid][0], team_lookup[tid][1], name, f"{pts:.2f}"]
                            ]

                    # Trade heuristic: owner changed between weeks in both directions for a pair
                    # Build mappings for quick check
                    prev_owner_by_pid = {
                        str(pid): str(bp.owner_team_id) for pid, bp in prev_players.items() if bp and bp.owner_team_id
                    }
                    curr_owner_by_pid = {
                        str(pid): str(bp.owner_team_id) for pid, bp in curr_players.items() if bp and bp.owner_team_id
                    }
                    prev_players_by_str = {str(pid): bp for pid, bp in prev_players.items()}
                    curr_players_by_str = {str(pid): bp for pid, bp in curr_players.items()}

                    # Aggregate acquired/sent per team
                    acquired_by_team = {}
                    sent_by_team = {}
                    for pid_str, prev_owner in prev_owner_by_pid.items():
                        curr_owner = curr_owner_by_pid.get(pid_str)
                        if curr_owner and prev_owner and curr_owner != prev_owner:
                            # owner changed this week
                            # points for current week
                            bp = curr_players_by_str.get(pid_str) or prev_players_by_str.get(pid_str)
                            pts = float(bp.points) if bp else 0.0
                            acquired_by_team.setdefault(curr_owner, []).append((pid_str, pts))
                            sent_by_team.setdefault(prev_owner, []).append((pid_str, pts))

                    # Compute weekly trades from normalized /FetchTrades events
                    trade_rows = []
                    for tr in events.get("trades", []):
                        team_id = str(tr.get("team_id"))
                        rec_ids = [str(x) for x in tr.get("players_received", [])]
                        sent_ids = [str(x) for x in tr.get("players_sent", [])]
                        # Exclude pick-only trades from weekly award
                        if not rec_ids or not sent_ids:
                            continue
                        recv_pts = sum(
                            float((curr_players.get(pid) or curr_players.get(int(pid))).points)
                            for pid in rec_ids
                            if (curr_players.get(pid) or curr_players.get(int(pid)))
                        )
                        sent_pts = sum(
                            float((curr_players.get(pid) or curr_players.get(int(pid))).points)
                            for pid in sent_ids
                            if (curr_players.get(pid) or curr_players.get(int(pid)))
                        )
                        net = recv_pts - sent_pts
                        def names(ids):
                            out = []
                            for pid in ids[:2]:
                                out.append(resolve_name(pid))
                            return ", ".join(out) if out else "—"
                        detail = f"{names(rec_ids)} vs {names(sent_ids)}"
                        if team_id in team_lookup:
                            tname, mgr = team_lookup[team_id]
                            trade_rows.append([tname, mgr, detail, f"{net:.2f}"])

                    if trade_rows:
                        trade_rows.sort(key=lambda r: float(r[-1]), reverse=True)
                        self.transactions_awards_best_trades = trade_rows[:1]
                        trade_rows.sort(key=lambda r: float(r[-1]))
                        self.transactions_awards_worst_trades = trade_rows[:1]
        except Exception as e:
            logger.debug(f"Transactions awards (worst start/sit) unavailable for week {week_counter}: {e}")

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ COUNT METRIC TIES ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        logger.debug("Counting metric ties.")

        # get number of scores ties and ties for first
        self.ties_for_scores = metrics_calculator.get_ties_count(self.data_for_scores, "score", self.break_ties)
        self.num_first_place_for_score_before_resolution = len(
            [list(group) for key, group in itertools.groupby(self.data_for_scores, lambda x: x[3])][0]
        )

        # reorder score data based on bench points if there are ties and break_ties = True
        if self.ties_for_scores > 0:
            self.data_for_scores = metrics_calculator.resolve_score_ties(self.data_for_scores, self.break_ties)
            metrics_calculator.get_ties_count(self.data_for_scores, "score", self.break_ties)
        self.num_first_place_for_score = len(
            [list(group) for key, group in itertools.groupby(self.data_for_scores, lambda x: x[3])][0]
        )

        # get number of coaching efficiency ties and ties for first
        self.ties_for_coaching_efficiency = metrics_calculator.get_ties_count(
            self.data_for_coaching_efficiency, "coaching_efficiency", self.break_ties
        )
        # Determine ties-at-first correctly by evaluating the top CE value, excluding DQs
        non_dq_efficiencies = [row for row in self.data_for_coaching_efficiency if row[3] != "DQ"]
        if non_dq_efficiencies:
            top_value = non_dq_efficiencies[0][3]
            self.num_first_place_for_coaching_efficiency_before_resolution = sum(
                1 for row in non_dq_efficiencies if row[3] == top_value
            )
        else:
            self.num_first_place_for_coaching_efficiency_before_resolution = 0

        if self.ties_for_coaching_efficiency > 0:
            self.data_for_coaching_efficiency = metrics_calculator.resolve_coaching_efficiency_ties(
                self.data_for_coaching_efficiency,
                self.ties_for_coaching_efficiency,
                league,
                self.teams_results,
                int(week_counter),
                int(week_for_report),
                self.break_ties,
            )
        self.num_first_place_for_coaching_efficiency = len(
            [list(group) for key, group in itertools.groupby(self.data_for_coaching_efficiency, lambda x: x[0])][0]
        )

        # get number of luck ties and ties for first
        self.ties_for_luck = metrics_calculator.get_ties_count(self.data_for_luck, "luck", self.break_ties)
        self.num_first_place_for_luck = len(
            [list(group) for key, group in itertools.groupby(self.data_for_luck, lambda x: x[3])][0]
        )

        # get number of bad boy rankings ties and ties for first
        self.ties_for_bad_boy_rankings = metrics_calculator.get_ties_count(
            self.data_for_bad_boy_rankings, "bad_boy", self.break_ties
        )
        self.num_first_place_for_bad_boy_rankings = len(
            [list(group) for key, group in itertools.groupby(self.data_for_bad_boy_rankings, lambda x: x[3])][0]
        )
        # filter out teams that have no bad boys in their starting lineup
        self.data_for_bad_boy_rankings = [result for result in self.data_for_bad_boy_rankings if int(result[-1]) != 0]

        # get number of beef rankings ties and ties for first
        self.ties_for_beef_rankings = metrics_calculator.get_ties_count(
            self.data_for_beef_rankings, "beef", self.break_ties
        )
        self.num_first_place_for_beef_rankings = len(
            [list(group) for key, group in itertools.groupby(self.data_for_beef_rankings, lambda x: x[3])][0]
        )

        # get number of high roller rankings ties and ties for first
        self.ties_for_high_roller_rankings = metrics_calculator.get_ties_count(
            self.data_for_high_roller_rankings, "high_roller", self.break_ties
        )
        self.num_first_place_for_high_roller_rankings = len(
            [list(group) for key, group in itertools.groupby(self.data_for_high_roller_rankings, lambda x: x[3])][0]
        )
        # filter out teams that have no high rollers in their starting lineup
        self.data_for_high_roller_rankings = [
            result for result in self.data_for_high_roller_rankings if float(result[3]) != 0.0
        ]

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ CALCULATE POWER RANKING ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        logger.debug("Calculating power rankings.")

        # calculate power ranking last to account for metric rankings that have been reordered due to tiebreakers
        power_ranking_results = metrics_calculator.calculate_power_rankings(
            self.teams_results, self.data_for_scores, self.data_for_coaching_efficiency, self.data_for_luck
        )

        # update data_for_teams with power rankings
        for team in self.data_for_teams:
            for team_id in power_ranking_results.keys():
                if team[0] == team_id:
                    team.append(power_ranking_results[team_id]["power_ranking"])

        # power rankings data
        self.data_for_power_rankings = []
        for k_v in sorted(power_ranking_results.items(), key=lambda x: x[1]["power_ranking"]):
            # Display power rank to two decimals for readability
            pretty_power_rank = f"{float(k_v[1]['power_ranking']):.2f}"
            self.data_for_power_rankings.append(
                [pretty_power_rank, power_ranking_results[k_v[0]]["name"], k_v[1]["manager_str"]]
            )

        # get number of power rankings ties and ties for first
        self.ties_for_power_rankings = metrics_calculator.get_ties_count(
            self.data_for_power_rankings, "power_ranking", self.break_ties
        )
        self.ties_for_first_for_power_rankings = len(
            [list(group) for key, group in itertools.groupby(self.data_for_power_rankings, lambda x: x[0])][0]
        )

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ LOGGER OUTPUT ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

        weekly_metrics_info = (
            f"\n~~~~~ WEEK {week_counter} METRICS INFO ~~~~~\n"
            f"              SCORE tie(s): {self.ties_for_scores}\n"
            f"COACHING EFFICIENCY tie(s): {self.ties_for_coaching_efficiency}\n"
        )

        # add line for coaching efficiency disqualifications if applicable
        ce_dq_str = None
        if self.num_coaching_efficiency_dqs > 0:
            ce_dqs = []
            for team_name, ineligible_players_count in self.coaching_efficiency_dqs.items():
                if ineligible_players_count == -1:
                    ce_dqs.append(f"{team_name} (incomplete active squad)")
                elif ineligible_players_count == -2:
                    ce_dqs.append(f"{team_name} (manually disqualified)")
                else:
                    ce_dqs.append(
                        f"{team_name} (ineligible bench players: "
                        f"{ineligible_players_count}/{league.roster_position_counts.get('BN')})"
                    )  # exclude IR

            ce_dq_str = ", ".join(ce_dqs)
            weekly_metrics_info += f"   COACHING EFFICIENCY DQs: {ce_dq_str}\n"

        # log weekly metrics info
        logger.debug(weekly_metrics_info)
        logger.info(
            f"Week {week_counter} data processed"
            f"{f' with the following coaching efficiency DQs: {ce_dq_str})' if ce_dq_str else ''}"
            f"."
        )
