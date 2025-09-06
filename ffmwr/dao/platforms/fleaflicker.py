__author__ = "Josh Bachler (fork maintainer); original: Wren J. R. (uberfastman)"
__email__ = "bakler5@gmail.com"

import datetime
import logging
import re
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Callable, Dict, Union

import requests
from bs4 import BeautifulSoup
from ffmwr.utilities.exceptions import NetworkError

from ffmwr.models.base.model import BaseManager, BaseMatchup, BasePlayer, BaseRecord, BaseStat, BaseTeam
from ffmwr.dao.platforms.base.platform import BasePlatform
from ffmwr.utilities.logger import get_logger
from ffmwr.utilities.settings import AppSettings

logger = get_logger(__name__, propagate=False)


# noinspection DuplicatedCode
class FleaflickerPlatform(BasePlatform):
    def __init__(
        self,
        settings: AppSettings,
        root_dir: Union[Path, None],
        data_dir: Path,
        league_id: str,
        season: int,
        start_week: int,
        week_for_report: int,
        get_current_nfl_week_function: Callable,
        week_validation_function: Callable,
        save_data: bool = True,
        offline: bool = False,
    ):
        super().__init__(
            settings,
            "Fleaflicker",
            "https://www.fleaflicker.com",
            root_dir,
            data_dir,
            league_id,
            season,
            start_week,
            week_for_report,
            get_current_nfl_week_function,
            week_validation_function,
            save_data,
            offline,
        )

    def _authenticate(self) -> None:
        pass

    def _scrape(self, url: str):
        logger.debug(f"Scraping Fleaflicker data from endpoint: {url}")

        user_agent = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0 "
            "Safari/605.1.15"
        )
        headers = {"user-agent": user_agent}
        try:
            response = self._request_with_retries("GET", url, headers=headers, timeout=20)
        except Exception as e:
            raise NetworkError(f"Failed to scrape Fleaflicker URL {url}: {e}")

        html_soup = BeautifulSoup(response.text, "html.parser")
        logger.debug(f"Response (HTML): {html_soup}")

        return html_soup

    def map_data_to_base(self) -> None:
        logger.debug(f"Retrieving {self.platform_display} league data and mapping it to base objects.")

        self.league.url = f"{self.base_url}/nfl/leagues/{self.league.league_id}"
        # ensure league season is set from requested season for correct historical attribution
        try:
            self.league.season = int(self.season) if self.season else self.league.season
        except Exception:
            self.league.season = self.season
        # scraped_league_info = self._scrape(self.league.url)

        scraped_league_scores = self._scrape(f"{self.league.url}/scores")

        try:
            scraped_current_week = (
                int(
                    scraped_league_scores.find_all(text=re.compile(".*This Week.*"))[-1]
                    .parent.findNext("li")
                    .text.strip()
                    .split(" ")[-1]
                )
                - 1
            )
        except (IndexError, AttributeError) as e:
            logger.error(f"Unable to scrape the current week: {e}")
            scraped_current_week = None

        scraped_league_rules = self._scrape(f"{self.league.url}/rules")

        elements = scraped_league_rules.find_all(["dt", "dd"])
        for elem in elements:
            if elem.text.strip() == "Playoffs":
                if elements[elements.index(elem) + 1].span:
                    self.league.num_playoff_slots = int(elements[elements.index(elem) + 1].span.text.strip())
                else:
                    self.league.num_playoff_slots = 0

                playoff_weeks_elements = elements[elements.index(elem) + 1].find_all(text=True, recursive=False)
                if any((text.strip() and "Weeks" in text) for text in playoff_weeks_elements):
                    for text in playoff_weeks_elements:
                        if text.strip() and "Weeks" in text:
                            for txt in text.split():
                                if "-" in txt:
                                    self.league.num_regular_season_weeks = int(txt.split("-")[0]) - 1
                elif self.league.num_playoff_slots == 0:
                    # TODO: figure out how to get total number of regular season weeks when league has no playoffs
                    self.league.num_regular_season_weeks = 18 if int(self.league.season) > 2020 else 17
                else:
                    self.league.num_regular_season_weeks = self.settings.num_regular_season_weeks
                break
            else:
                self.league.num_playoff_slots = self.settings.num_playoff_slots
                self.league.num_regular_season_weeks = self.settings.num_regular_season_weeks

        # TODO: how to get league rules for LAST YEAR from Fleaflicker API
        league_rules = self.query(f"https://www.fleaflicker.com/api/FetchLeagueRules?leagueId={self.league.league_id}")

        league_standings = self.query(
            f"https://www.fleaflicker.com/api/FetchLeagueStandings"
            f"?leagueId={self.league.league_id}{f'&season={self.league.season}' if self.league.season else ''}"
        )

        league_info = league_standings.get("league")

        league_teams = {}
        ranked_league_teams = []
        for division in league_standings.get("divisions"):
            self.league.divisions[str(division.get("id"))] = division.get("name")
            self.league.num_divisions += 1
            for team in division.get("teams"):
                team["division_id"] = division.get("id")
                team["division_name"] = division.get("name")
                league_teams[team.get("id")] = team
                ranked_league_teams.append(team)

        ranked_league_teams.sort(
            key=lambda x: x.get("recordOverall").get("rank") if x.get("recordOverall").get("rank") else 0
        )

        median_score_by_week = {}
        matchups_by_week = {}
        # Build explicit week start timestamps from the league scoreboard
        week_start_ts: Dict[int, int] = {}
        for wk in range(self.start_week, int(self.league.num_regular_season_weeks) + 1):
            matchups_by_week[str(wk)] = self.query(
                f"https://www.fleaflicker.com/api/FetchLeagueScoreboard"
                f"?leagueId={self.league.league_id}&scoringPeriod={wk}"
                f"{f'&season={self.league.season}' if self.league.season else ''}"
            )
            try:
                games = matchups_by_week[str(wk)].get("games") or []
                logger.debug(f"Scoreboard: week {wk} games={len(games)}")
            except Exception:
                logger.debug(f"Scoreboard: week {wk} games=unknown (unexpected payload shape)")
            try:
                sp = matchups_by_week[str(wk)].get("schedulePeriod", {})
                low = sp.get("low", {})
                start_ms = low.get("startEpochMilli")
                if start_ms:
                    week_start_ts[int(wk)] = int(start_ms)
            except Exception:
                pass
        # Log derived week windows for visibility (does not gate median calculation)
        if week_start_ts:
            try:
                sorted_weeks = sorted(week_start_ts.keys())
                debug_lines = []
                for i, w in enumerate(sorted_weeks):
                    start = week_start_ts[w]
                    if i + 1 < len(sorted_weeks):
                        end = week_start_ts[sorted_weeks[i + 1]] - 1
                    else:
                        end = start + (8 * 24 * 60 * 60 * 1000)
                    debug_lines.append(
                        f"W{w}: start={start} end={end} ({datetime.datetime.fromtimestamp(start/1000):%Y-%m-%d} to "
                        f"{datetime.datetime.fromtimestamp(end/1000):%Y-%m-%d})"
                    )
                logger.info(
                    f"Fleaflicker week windows for season {self.league.season}:\n  " + "\n  ".join(debug_lines)
                )
            except Exception:
                pass

        # Compute weekly medians for every week up to the report week
        for wk in range(self.start_week, min(self.league.week_for_report, int(self.league.num_regular_season_weeks)) + 1):
            week_key = str(wk)
            games = (matchups_by_week.get(week_key) or {}).get("games") or []
            scores = []
            for matchup in games:
                for key in ["home", "away"]:
                    score_val = None
                    side = key + "Score"
                    score_obj = matchup.get(side) or {}
                    parsed = None
                    if isinstance(score_obj, dict):
                        parsed = (
                            (score_obj.get("score") or {}).get("value")
                            or score_obj.get("value")
                            or score_obj.get("formatted")
                        )
                    try:
                        score_val = float(parsed) if parsed is not None else None
                    except (TypeError, ValueError):
                        score_val = None
                    if score_val is not None:
                        scores.append(score_val)
                    else:
                        logger.debug(
                            f"Score parse miss: week {wk} side={key} raw={score_obj} parsed={parsed} -> None"
                        )

            weekly_median = round(median(scores), 2) if scores else None
            median_score_by_week[week_key] = weekly_median
            logger.info(
                f"Median: week {wk} scores={len(scores)} median={weekly_median if weekly_median is not None else 'N/A'}"
            )

        rosters_by_week = {}
        # expose median scores by week on league for downstream display/logging
        try:
            self.league.median_score_by_week = {k: v for k, v in median_score_by_week.items()}
        except Exception:
            pass
        for wk in range(self.start_week, self.league.week_for_report + 1):
            rosters_by_week[str(wk)] = {
                str(team.get("id")): self.query(
                    f"https://www.fleaflicker.com/api/FetchRoster"
                    f"?leagueId={self.league.league_id}&teamId={team.get('id')}&scoringPeriod={wk}"
                    f"{f'&season={self.league.season}' if self.league.season else ''}"
                )
                for team in ranked_league_teams
            }

        # Fetch league activity (adds/claims/drops/trades) with pagination
        activity_items = []
        try:
            base_act_url = f"https://www.fleaflicker.com/api/FetchLeagueActivity?leagueId={self.league.league_id}"
            result_offset = None
            while True:
                url = base_act_url if not result_offset else f"{base_act_url}&resultOffset={result_offset}"
                page = self.query(url)
                activity_items.extend(page.get("items", []) or [])
                result_offset = page.get("resultOffsetNext")
                if not result_offset:
                    break
        except Exception as e:
            logger.debug(f"FetchLeagueActivity pagination failed: {e}")

        # initialize normalized transactions per week
        for wk in range(self.start_week, int(self.league.num_regular_season_weeks) + 1):
            self.league.transactions_by_week[str(wk)] = {"adds": [], "claims": [], "drops": [], "trades": []}

        league_transactions_by_team = defaultdict(dict)
        for activity in activity_items:
            epoch_milli = float(activity.get("timeEpochMilli"))
            timestamp = datetime.datetime.fromtimestamp(epoch_milli / 1000)

            season_start = datetime.datetime(self.league.season, 9, 1)
            season_end = datetime.datetime(self.league.season + 1, 3, 1)

            if season_start < timestamp < season_end:
                tx = activity.get("transaction")
                if tx:
                    transaction_type = tx.get("type") or "TRANSACTION_ADD"

                    is_move = False
                    is_trade = False
                    if "TRADE" in transaction_type:
                        is_trade = True
                    elif any(transaction_str in transaction_type for transaction_str in ["CLAIM", "ADD", "DROP"]):
                        is_move = True

                    team_id = str((tx.get("team") or {}).get("id"))
                    # determine player and period/week
                    pro_player_id = None
                    week_ord = None
                    player_block = tx.get("player")
                    if player_block:
                        pro_player_id = str((player_block.get("proPlayer") or {}).get("id"))
                        try:
                            req_games = player_block.get("requestedGames", [])
                            if req_games:
                                week_ord = int(req_games[0].get("period", {}).get("ordinal"))
                        except Exception:
                            week_ord = None
                    if not pro_player_id and tx.get("players"):
                        try:
                            pro_player_id = str((tx.get("players")[0].get("proPlayer") or {}).get("id"))
                        except Exception:
                            pro_player_id = None
                    # validate or derive week ordinal using epoch time and windows
                    if week_start_ts:
                        try:
                            ems = int(float(activity.get("timeEpochMilli", 0)))
                            # compute end as next week's start - 1, or +8 days for final
                            sorted_weeks = sorted(week_start_ts.keys())
                            # function-like inline to map epoch to window
                            def _map_epoch(ts_ms: int):
                                for i, w in enumerate(sorted_weeks):
                                    start = week_start_ts[w]
                                    if i + 1 < len(sorted_weeks):
                                        end = week_start_ts[sorted_weeks[i + 1]] - 1
                                    else:
                                        end = start + (8 * 24 * 60 * 60 * 1000)
                                    if start <= ts_ms <= end:
                                        return w
                                return None

                            mapped = _map_epoch(ems)
                            if week_ord is None:
                                week_ord = mapped
                                if week_ord is not None:
                                    logger.info(
                                        f"Activity map: type={transaction_type} team={team_id} pid={pro_player_id} "
                                        f"ems={ems} ({datetime.datetime.fromtimestamp(ems/1000):%Y-%m-%d %H:%M}) -> week={week_ord}"
                                    )
                            else:
                                # validate RG ordinal; if mismatched with epoch window, override with mapped
                                if mapped is not None and mapped != week_ord:
                                    logger.info(
                                        f"Activity ordinal mismatch: ordinal={week_ord} epoch->week={mapped}; overriding"
                                    )
                                    week_ord = mapped
                        except Exception:
                            pass
                    if not week_ord:
                        try:
                            ems = int(float(activity.get("timeEpochMilli", 0)))
                            logger.info(
                                f"Activity skip (no week match): type={transaction_type} team={team_id} pid={pro_player_id} ts={ems} ({datetime.datetime.fromtimestamp(ems/1000):%Y-%m-%d %H:%M})"
                            )
                        except Exception:
                            pass
                        continue

                    # record normalized weekly activity for awards
                    if pro_player_id and team_id and str(week_ord) in self.league.transactions_by_week:
                        wk_key = str(week_ord)
                        if "DROP" in transaction_type:
                            self.league.transactions_by_week[wk_key]["drops"].append(
                                {"team_id": team_id, "player_id": pro_player_id}
                            )
                        elif "CLAIM" in transaction_type:
                            self.league.transactions_by_week[wk_key]["claims"].append(
                                {"team_id": team_id, "player_id": pro_player_id}
                            )
                        elif "ADD" in transaction_type:
                            self.league.transactions_by_week[wk_key]["adds"].append(
                                {"team_id": team_id, "player_id": pro_player_id}
                            )
                        elif "TRADE" in transaction_type:
                            self.league.transactions_by_week[wk_key]["trades"].append(activity)

                    if not league_transactions_by_team[team_id]:
                        league_transactions_by_team[team_id] = {
                            "transactions": [transaction_type],
                            "moves": 1 if is_move else 0,
                            "trades": 1 if is_trade else 0,
                        }
                    else:
                        league_transactions_by_team[team_id]["transactions"].append(transaction_type)
                        league_transactions_by_team[team_id]["moves"] += 1 if is_move else 0
                        league_transactions_by_team[team_id]["trades"] += 1 if is_trade else 0

        # Fetch completed trades and normalize per-week team events
        try:
            base_trades_url = (
                f"https://www.fleaflicker.com/api/FetchTrades?leagueId={self.league.league_id}&filter=TRADES_COMPLETED"
            )
            trades = []
            result_offset = None
            # paginate as needed
            while True:
                url = base_trades_url if not result_offset else f"{base_trades_url}&resultOffset={result_offset}"
                resp = self.query(url)
                trades.extend(resp.get("trades", []) or [])
                result_offset = resp.get("resultOffsetNext")
                if not result_offset:
                    break

            # Normalize trades into league.transactions_by_week
            earliest_start_ts = min(week_start_ts.values()) if week_start_ts else None
            for tr in trades:
                teams = tr.get("teams") or []
                trade_id = tr.get("id")
                # capture trade timestamp when available for downstream filtering/debug
                trade_ts = None
                for ts_key in ("approvedOn", "proposedOn", "tentative_execution_time", "executionTimeEpochMilli"):
                    try:
                        val = tr.get(ts_key)
                        if val:
                            trade_ts = int(float(val))
                            break
                    except Exception:
                        continue
                # Precompute each team's received ids
                team_received = []  # list of (team_id, [player_ids], week_guess)
                for entry in teams:
                    team_id = str((entry.get("team") or {}).get("id"))
                    rec_ids = []
                    week_guess = None
                    for p in entry.get("playersObtained", []) or []:
                        pro = (p.get("proPlayer") or {})
                        pid = pro.get("id")
                        if pid is not None:
                            rec_ids.append(str(pid))
                        # Prefer mapping by trade_ts; if unavailable, derive from requestedGames start time
                        if week_start_ts and (trade_ts or not week_guess):
                            try:
                                def _map_epoch(ts_ms: int):
                                    if not ts_ms:
                                        return None
                                    sw = sorted(week_start_ts.keys())
                                    for i, w in enumerate(sw):
                                        start = week_start_ts[w]
                                        end = week_start_ts[sw[i + 1]] - 1 if i + 1 < len(sw) else start + (8 * 24 * 60 * 60 * 1000)
                                        if start <= ts_ms <= end:
                                            return w
                                    return None
                                mapped = _map_epoch(trade_ts) if trade_ts else None
                                if mapped is None:
                                    req_games = p.get("requestedGames", [])
                                    if req_games:
                                        start_ms = req_games[0].get("period", {}).get("startEpochMilli")
                                        mapped = _map_epoch(int(start_ms)) if start_ms else None
                                if mapped is not None:
                                    week_guess = mapped
                                # Offseason: if still unmapped but we have a timestamp before week 1, map to week 1
                                if (
                                    week_guess is None
                                    and trade_ts is not None
                                    and earliest_start_ts is not None
                                    and int(trade_ts) < int(earliest_start_ts)
                                ):
                                    week_guess = self.start_week
                            except Exception:
                                pass
                    if rec_ids and team_id:
                        # if we cannot determine the period ordinal for any player, skip this trade week attribution
                        if not week_guess:
                            continue
                        team_received.append((team_id, rec_ids, int(week_guess)))

                # Construct sent lists as union of other teams' received
                for idx, (team_id, rec_ids, week_guess) in enumerate(team_received):
                    sent_ids = []
                    for jdx, (other_team, other_rec_ids, _) in enumerate(team_received):
                        if jdx == idx:
                            continue
                        sent_ids.extend(other_rec_ids)
                    wk_key = str(week_guess)
                    if wk_key in self.league.transactions_by_week:
                        self.league.transactions_by_week[wk_key]["trades"].append(
                            {
                                "team_id": team_id,
                                "players_received": rec_ids,
                                "players_sent": sent_ids,
                                "trade_ts": trade_ts,
                                "trade_id": trade_id,
                            }
                        )
        except Exception as e:
            logger.debug(f"FetchTrades normalization failed: {e}")

        # Per-team recent transactions (last N, paginated) -> cache by team and by week
        try:
            for tm in ranked_league_teams:
                team_id = str(tm.get("id"))
                team_events_by_week = {}

                # check offline cache first
                if self.league.offline:
                    # attempt to load cached per-week files and hydrate transactions_by_week
                    for wk in range(self.start_week, self.league.week_for_report + 1):
                        wk_key = str(wk)
                        cache_path = self.league.data_dir / f"week_{wk}" / f"team_{team_id}_transactions.json"
                        if cache_path.is_file():
                            import json
                            try:
                                with open(cache_path, "r", encoding="utf-8") as fh:
                                    cached = json.load(fh)
                                team_events_by_week[wk_key] = cached
                            except Exception:
                                pass
                    # hydrate league-level structure
                    for wk_key, evs in team_events_by_week.items():
                        for ev in evs:
                            self.league.transactions_by_week[wk_key][ev["kind"]].append(
                                {"team_id": team_id, "player_id": ev.get("player_id")}
                            )
                    continue

                base_team_tx_url = (
                    f"https://www.fleaflicker.com/api/FetchLeagueTransactions?sport=NFL&league_id={self.league.league_id}&team_id={team_id}"
                )
                result_offset = None
                loops = 0
                # establish earliest season start to stop paging once we are before season
                try:
                    earliest_start = min(week_start_ts.values()) if week_start_ts else None
                except Exception:
                    earliest_start = None
                while True:
                    loops += 1
                    url = base_team_tx_url if not result_offset else f"{base_team_tx_url}&resultOffset={result_offset}"
                    page = self.query(url)
                    items = page.get("items", [])
                    if not items:
                        break
                    # track the oldest item on this page to decide when to stop paging
                    oldest_ts = None
                    for it in items:
                        ts = int(float(it.get("timeEpochMilli", "0")))
                        tx = it.get("transaction", {})
                        ttype = tx.get("type") or "TRANSACTION_ADD"  # default to ADD when missing
                        pblock = tx.get("player") or {}
                        pid = (pblock.get("proPlayer") or {}).get("id")
                        # derive week
                        wk_ord = None
                        rg = pblock.get("requestedGames") or []
                        if rg:
                            try:
                                wk_ord = int(rg[0].get("period", {}).get("ordinal"))
                            except Exception:
                                wk_ord = None
                        if week_start_ts:
                            try:
                                ems = ts
                                sorted_weeks = sorted(week_start_ts.keys())
                                def _map_epoch(ts_ms: int):
                                    for i, w in enumerate(sorted_weeks):
                                        start = week_start_ts[w]
                                        if i + 1 < len(sorted_weeks):
                                            end = week_start_ts[sorted_weeks[i + 1]] - 1
                                        else:
                                            end = start + (8 * 24 * 60 * 60 * 1000)
                                        if start <= ts_ms <= end:
                                            return w
                                    return None
                                mapped = _map_epoch(ems)
                                if wk_ord is None:
                                    wk_ord = mapped
                                elif mapped is not None and mapped != wk_ord:
                                    logger.info(
                                        f"TeamTx ordinal mismatch: ordinal={wk_ord} epoch->week={mapped}; overriding"
                                    )
                                    wk_ord = mapped
                            except Exception:
                                pass
                        if not wk_ord:
                            continue

                        wk_key = str(wk_ord)
                        if wk_ord < self.start_week or wk_ord > self.league.num_regular_season_weeks:
                            continue
                        kind = (
                            "drops"
                            if "DROP" in ttype
                            else ("claims" if "CLAIM" in ttype else ("adds" if "ADD" in ttype else None))
                        )
                        if kind and pid is not None:
                            event = {"kind": kind, "player_id": str(pid)}
                            team_events_by_week.setdefault(wk_key, []).append(event)
                            # also hydrate league-level structure
                            self.league.transactions_by_week[wk_key][kind].append(
                                {"team_id": team_id, "player_id": str(pid)}
                            )
                            logger.info(
                                f"TeamTx map: team={team_id} kind={kind} pid={pid} ts={ts} ({datetime.datetime.fromtimestamp(ts/1000):%Y-%m-%d %H:%M}) -> week={wk_ord}"
                            )
                        # track oldest ts on page
                        if oldest_ts is None or ts < oldest_ts:
                            oldest_ts = ts

                    result_offset = page.get("resultOffsetNext")
                    # stop when there are no more pages or we have paged prior to season start
                    if not result_offset:
                        break
                    if earliest_start and oldest_ts and oldest_ts < earliest_start:
                        break

                # write cache per team per week
                try:
                    if team_events_by_week and self.league.save_data:
                        import json
                        for wk_key, evs in team_events_by_week.items():
                            wk_dir = self.league.data_dir / f"week_{wk_key}"
                            wk_dir.mkdir(parents=True, exist_ok=True)
                            cache_path = wk_dir / f"team_{team_id}_transactions.json"
                            with open(cache_path, "w", encoding="utf-8") as fh:
                                json.dump(evs, fh, ensure_ascii=False)
                except Exception as e:
                    logger.debug(f"Failed to write per-team transactions cache for team {team_id}: {e}")
        except Exception as e:
            logger.debug(f"Per-team transaction fetch failed: {e}")

        # Final summary by week (adds/claims/drops/trades)
        try:
            for w in range(self.start_week, self.league.week_for_report + 1):
                wk_key = str(w)
                adds = len(self.league.transactions_by_week[wk_key]["adds"]) if wk_key in self.league.transactions_by_week else 0
                claims = len(self.league.transactions_by_week[wk_key]["claims"]) if wk_key in self.league.transactions_by_week else 0
                drops = len(self.league.transactions_by_week[wk_key]["drops"]) if wk_key in self.league.transactions_by_week else 0
                trades = len(self.league.transactions_by_week[wk_key]["trades"]) if wk_key in self.league.transactions_by_week else 0
                logger.info(
                    f"Tx summary week {w}: adds={adds} claims={claims} drops={drops} trades={trades}"
                )
        except Exception:
            pass

        self.league.name = league_info.get("name")
        self.league.week = int(scraped_current_week) if scraped_current_week else self.current_week
        # TODO: figure out how to get league starting week
        self.league.start_week = self.start_week
        self.league.num_teams = int(league_info.get("size"))
        self.league.has_divisions = self.league.num_divisions > 0
        # TODO: FIGURE OUT WHERE FLEAFLICKER EXPOSES THIS! Fleaflicker supports both MEDIAN and MEAN games
        self.league.has_median_matchup = False
        self.league.median_score = 0
        self.league.faab_budget = int(league_info.get("defaultWaiverBudget", 0))
        self.league.is_faab = self.league.faab_budget > 0

        # self.league.player_data_by_week_function = None
        # self.league.player_data_by_week_key = None

        for position in league_rules.get("rosterPositions"):
            pos_attributes = self.position_mapping.get(position.get("label"))
            pos_name = pos_attributes.get("base")
            if position.get("start"):
                pos_count = int(position.get("start"))
            elif position.get("label") == "BN":
                pos_count = int(position.get("max")) if position.get("max") else 0
            else:
                pos_count = 0

            if pos_attributes.get("is_flex"):
                self.league.__setattr__(
                    pos_attributes.get("league_positions_attribute"), pos_attributes.get("positions")
                )

            self.league.roster_positions.append(pos_name)
            self.league.roster_position_counts[pos_name] = pos_count
            self.league.roster_active_slots.extend(
                [pos_name] * pos_count if pos_name not in self.league.bench_positions else []
            )

        league_median_records_by_team = {}
        for week, matchups in matchups_by_week.items():
            matchups_week = matchups.get("schedulePeriod").get("value")
            matchups = matchups.get("games")

            self.league.teams_by_week[str(week)] = {}
            self.league.matchups_by_week[str(week)] = []

            for matchup in matchups:
                base_matchup = BaseMatchup()

                base_matchup.week = int(matchups_week)
                base_matchup.complete = True if bool(matchup.get("isFinalScore")) else False
                base_matchup.tied = True if matchup.get("homeResult") == "TIE" else False

                for key in ["home", "away"]:
                    team_data: Dict = matchup.get(key)
                    base_team = BaseTeam()

                    opposite_key = "away" if key == "home" else "home"
                    team_division = league_teams[team_data.get("id")].get("division_id")
                    opponent_division = league_teams[matchup.get(opposite_key).get("id")].get("division_id")
                    if team_division and opponent_division and team_division == opponent_division:
                        base_matchup.division_matchup = True

                    base_team.week = int(matchups_week)
                    base_team.name = team_data.get("name")

                    managers = league_teams[team_data.get("id")].get("owners")
                    if managers:
                        for manager in managers:
                            base_manager = BaseManager()

                            base_manager.manager_id = str(manager.get("id"))
                            base_manager.email = None
                            base_manager.name = manager.get("displayName")

                            base_team.managers.append(base_manager)

                    base_team.manager_str = ", ".join([manager.name_str for manager in base_team.managers])

                    base_team.team_id = str(team_data.get("id"))
                    base_team.points = float(matchup.get(key + "Score", {}).get("score", {}).get("value", 0))
                    base_team.projected_points = None

                    # TODO: currently the fleaflicker API call only returns 1st PAGE of transactions... figure this out!
                    base_team.num_moves = f"{league_transactions_by_team[str(base_team.team_id)].get('moves', 0)}*"
                    base_team.num_trades = f"{league_transactions_by_team[str(base_team.team_id)].get('trades', 0)}*"

                    base_team.waiver_priority = team_data.get("waiverPosition", 0)
                    self.league.has_waiver_priorities = base_team.waiver_priority > 0
                    base_team.faab = team_data.get("waiverAcquisitionBudget", {}).get("value", 0)
                    base_team.url = (
                        f"https://www.fleaflicker.com"
                        f"/nfl/leagues/{self.league.league_id}/teams/{str(team_data.get('id'))}"
                    )

                    if team_data.get("streak").get("value"):
                        if team_data.get("streak").get("value") > 0:
                            streak_type = "W"
                        elif team_data.get("streak").get("value") < 0:
                            streak_type = "L"
                        else:
                            streak_type = "T"
                    else:
                        streak_type = "T"

                    base_team.division = team_division
                    base_team.current_record = BaseRecord(
                        wins=int(team_data.get("recordOverall", {}).get("wins", 0)),
                        losses=int(team_data.get("recordOverall", {}).get("losses", 0)),
                        ties=int(team_data.get("recordOverall", {}).get("ties", 0)),
                        percentage=round(
                            float(team_data.get("recordOverall", {}).get("winPercentage", {}).get("value", 0)), 3
                        ),
                        points_for=float(team_data.get("pointsFor", {}).get("value", 0)),
                        points_against=float(team_data.get("pointsAgainst", {}).get("value", 0)),
                        streak_type=streak_type,
                        streak_len=int(abs(team_data.get("streak", {}).get("value", 0))),
                        team_id=base_team.team_id,
                        team_name=base_team.name,
                        rank=int(team_data.get("recordOverall", {}).get("rank", 0)),
                        division=base_team.division,
                        division_wins=int(team_data.get("recordDivision", {}).get("wins", 0)),
                        division_losses=int(team_data.get("recordDivision", {}).get("losses", 0)),
                        division_ties=int(team_data.get("recordDivision", {}).get("ties", 0)),
                        division_percentage=round(
                            float(team_data.get("recordDivision", {}).get("winPercentage", {}).get("value", 0)), 3
                        ),
                        division_rank=int(team_data.get("recordDivision", {}).get("rank", 0)),
                    )
                    base_team.streak_str = base_team.current_record.get_streak_str()
                    if base_matchup.division_matchup:
                        base_team.division_streak_str = base_team.current_record.get_division_streak_str()

                    # get median for week
                    week_median = median_score_by_week.get(str(week))

                    median_record: BaseRecord = league_median_records_by_team.get(str(base_team.team_id))

                    if not median_record:
                        median_record = BaseRecord(team_id=base_team.team_id, team_name=base_team.name)
                        league_median_records_by_team[str(base_team.team_id)] = median_record

                    if week_median is not None:
                        # use this if you want the tie-break to be season total points over/under median score
                        median_record.add_points_for(base_team.points - week_median)
                        # use this if you want the tie-break to be current week points over/under median score
                        # median_record.add_points_for(
                        #     (median_record.get_points_for() * -1) + (base_team.points - week_median))
                        median_record.add_points_against((median_record.get_points_against() * -1) + week_median)
                        if base_team.points > week_median:
                            median_record.add_win()
                        elif base_team.points < week_median:
                            median_record.add_loss()
                        else:
                            median_record.add_tie()

                        base_team.current_median_record = median_record
                    else:
                        logger.debug(
                            f"Median missing: week={week} team_id={base_team.team_id} team_pts={base_team.points}"
                        )

                    # add team to matchup teams
                    base_matchup.teams.append(base_team)

                    # add team to league teams by week
                    self.league.teams_by_week[str(week)][str(base_team.team_id)] = base_team

                    # no winner/loser if matchup is tied
                    if matchup.get(key + "Result") == "WIN":
                        base_matchup.winner = base_team
                    elif matchup.get(key + "Result") == "LOSE":
                        base_matchup.loser = base_team

                # add matchup to league matchups by week
                self.league.matchups_by_week[str(week)].append(base_matchup)

        for week, rosters in rosters_by_week.items():
            self.league.players_by_week[str(week)] = {}
            for team_id, roster in rosters.items():
                league_team: BaseTeam = self.league.teams_by_week.get(str(week)).get(str(team_id))

                for player in [slot for group in roster.get("groups") for slot in group.get("slots")]:
                    flea_player_position = player.get("position")
                    flea_league_player = player.get("leaguePlayer")

                    # noinspection SpellCheckingInspection
                    if flea_league_player:
                        flea_pro_player = flea_league_player.get("proPlayer")

                        base_player = BasePlayer()

                        base_player.week_for_report = int(week)
                        base_player.player_id = flea_pro_player.get("id")
                        base_player.bye_week = int(flea_pro_player.get("nflByeWeek", 0))
                        # TODO: jersey number only appears to be available in player profile
                        # flea_player_profile = self.query(
                        #     f"https://www.fleaflicker.com/api/FetchPlayerProfile?"
                        #     f"leagueId={self.league.league_id}"
                        #     f"&playerId={flea_pro_player.get('id')}"
                        # )
                        # base_player.jersey_number = flea_player_profile.get("detail").get("jerseyNumber")
                        base_player.display_position = self.get_mapped_position(flea_pro_player.get("position"))
                        base_player.nfl_team_id = None
                        base_player.nfl_team_abbr = flea_pro_player.get("proTeam", {}).get("abbreviation").upper()
                        base_player.nfl_team_name = (
                            f"{flea_pro_player.get('proTeam', {}).get('location')} "
                            f"{flea_pro_player.get('proTeam', {}).get('name')}"
                        )

                        if flea_player_position.get("label") == "D/ST":
                            base_player.first_name = flea_pro_player.get("nameFull")
                            # use ESPN D/ST team logo (higher resolution) because Fleaflicker does not provide them
                            base_player.headshot_url = f"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nfl/500/{base_player.nfl_team_abbr}.png"
                        else:
                            base_player.first_name = flea_pro_player.get("nameFirst")
                            base_player.last_name = flea_pro_player.get("nameLast")
                            base_player.headshot_url = flea_pro_player.get("headshotUrl")

                        base_player.full_name = flea_pro_player.get("nameFull")
                        base_player.owner_team_id = flea_league_player.get("owner", {}).get("id")
                        base_player.owner_team_name = flea_league_player.get("owner", {}).get("name")
                        base_player.percent_owned = 0
                        base_player.points = float(flea_league_player.get("viewingActualPoints", {}).get("value", 0))
                        # TODO: get season total points via summation, since this gives the end of season total, not
                        #  the total as of the selected week
                        # base_player.season_points = float(flea_league_player.get("seasonTotal", {}).get("value", 0))
                        # base_player.season_average_points = round(float(
                        #     flea_league_player.get("seasonAverage", {}).get("value", 0)), 2)
                        base_player.projected_points = None

                        base_player.position_type = (
                            "O"
                            if self.get_mapped_position(flea_pro_player.get("position"))
                            in self.league.offensive_positions
                            else "D"
                        )
                        base_player.primary_position = self.get_mapped_position(flea_pro_player.get("position"))

                        eligible_positions = [
                            position
                            for position in flea_league_player.get("proPlayer", {}).get("positionEligibility", [])
                        ]
                        for position in eligible_positions:
                            base_position = self.get_mapped_position(position)
                            base_player.eligible_positions.add(base_position)
                            for flex_position, positions in self.league.get_flex_positions_dict().items():
                                if base_position in positions:
                                    base_player.eligible_positions.add(flex_position)

                        base_player.selected_position = self.get_mapped_position(flea_player_position.get("label"))
                        base_player.selected_position_is_flex = self.position_mapping.get(
                            flea_pro_player.get("position")
                        ).get("is_flex")

                        # typeAbbreviaition is misspelled in API data
                        # noinspection SpellCheckingInspection
                        base_player.status = flea_pro_player.get("injury", {}).get("typeAbbreviaition")

                        for stat in flea_league_player.get("viewingActualStats"):
                            base_stat = BaseStat()

                            base_stat.stat_id = stat.get("category", {}).get("id")
                            base_stat.name = stat.get("category", {}).get("abbreviation")
                            base_stat.value = stat.get("value", {}).get("value", 0)

                            base_player.stats.append(base_stat)

                        # add player to team roster
                        league_team.roster.append(base_player)

                        # add player to league players by week
                        self.league.players_by_week[str(week)][base_player.player_id] = base_player

        # fetch free agents per week up to week_for_report for Best of the Rest feature
        for wk in range(self.start_week, self.league.week_for_report + 1):
            try:
                self.league.free_agents_by_week[str(wk)] = self._fetch_free_agents_for_week(wk)
            except Exception as e:
                logger.debug(f"Unable to retrieve free agents for week {wk}: {e}")
                self.league.free_agents_by_week[str(wk)] = {}

        self.league.current_standings = sorted(
            self.league.teams_by_week.get(str(self.league.week_for_report)).values(),
            key=lambda x: x.current_record.rank,
        )

        # Ensure the cumulative median record is attached to the week_for_report team objects even
        # when the current week median is not available (e.g., prerun). This carries forward prior weeks.
        try:
            wfr_key = str(self.league.week_for_report)
            for team_id, med_rec in league_median_records_by_team.items():
                team_map = self.league.teams_by_week.get(wfr_key, {})
                team_obj = team_map.get(str(team_id))
                if team_obj:
                    team_obj.current_median_record = med_rec
        except Exception:
            pass

        self.league.current_median_standings = sorted(
            self.league.teams_by_week.get(str(self.league.week_for_report)).values(),
            key=lambda x: (
                x.current_median_record.get_wins(),
                -x.current_median_record.get_losses(),
                x.current_median_record.get_ties(),
                x.current_median_record.get_points_for(),
            ),
            reverse=True,
        )

    def _fetch_free_agents_for_week(self, week: int) -> Dict[str, BasePlayer]:
        """Retrieve league free agents and map to BasePlayer for a specific week.

        Uses Fleaflicker public API endpoint to fetch players marked as free agents for the league and scoring period.
        This is a best-effort implementation; endpoint parameters may evolve.
        """
        free_agents: Dict[str, BasePlayer] = {}
        # cache location
        week_dir = self.league.data_dir / f"week_{week}"
        cache_path = week_dir / "free_agents.json"

        logger.info(f"Retrieving Fleaflicker free agents for week {week}...")

        # Prefer cache when available to avoid re-querying previous weeks
        if cache_path.is_file():
            try:
                import json
                with open(cache_path, "r", encoding="utf-8") as fh:
                    cached = json.load(fh)
                for pid, pdata in cached.items():
                    bp = BasePlayer()
                    bp.week_for_report = int(week)
                    bp.player_id = pid
                    bp.full_name = pdata.get("full_name")
                    bp.first_name = pdata.get("first_name")
                    bp.last_name = pdata.get("last_name")
                    bp.nfl_team_abbr = pdata.get("nfl_team_abbr")
                    bp.display_position = pdata.get("primary_position")
                    bp.primary_position = pdata.get("primary_position")
                    for pos in pdata.get("eligible_positions", []):
                        bp.eligible_positions.add(pos)
                    bp.points = float(pdata.get("points", 0))
                    free_agents[str(pid)] = bp
                logger.info(
                    f"...loaded {len(free_agents)} cached Fleaflicker free agents for week {week} from {cache_path}."
                )
                return free_agents
            except Exception as e:
                logger.debug(f"Failed to load cached free agents for week {week}: {e}")
        base_url_primary = f"{self.base_url}/api/FetchPlayerListing"
        base_url_fallback = f"{self.base_url}/api/FetchPlayers"
        headers = {
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15"
            ),
            "accept": "application/json, text/plain, */*",
        }

        result_offset = 0
        page_size = 30  # FetchPlayerListing returns 30 per page by default
        total_found = 0
        # try primary documented endpoint first (use minimal params proven by curl)
        using_fallback = False
        use_camelcase_params = False  # switch to camelCase params for Listing if snake_case fails
        while True:
            # Build minimal params for Listing in either snake_case or camelCase
            # First page: no offset params. Subsequent pages: include offset (and limit for safety)
            params_snake = {
                "sport": "NFL",
                "league_id": str(self.league.league_id),
                "filter.free_agent_only": "true",
                "sort": "SORT_SCORING_PERIOD",
                "sort_period": str(week),
                "sort_season": str(self.league.season),
            }
            if result_offset > 0:
                # Only pass result_offset; result_limit has caused 400s on some leagues
                params_snake.update({
                    "result_offset": str(result_offset),
                })

            params_camel = {
                "sport": "NFL",
                "leagueId": str(self.league.league_id),
                "filter.freeAgentOnly": "true",
                "sort": "SORT_SCORING_PERIOD",
                "sortPeriod": str(week),
                "sortSeason": str(self.league.season),
            }
            if result_offset > 0:
                # Only pass resultOffset; avoid resultLimit due to 400s observed
                params_camel.update({
                    "resultOffset": str(result_offset),
                })
            params = params_camel if use_camelcase_params else params_snake
            target_url = base_url_primary if not using_fallback else base_url_fallback
            # translate params for fallback endpoint if used
            fallback_params = {
                "sport": "NFL",
                "leagueId": str(self.league.league_id),
                "filter.freeAgentOnly": "true",
                "scoringPeriod": str(week),
                "resultOffset": str(result_offset),
                "resultLimit": str(page_size),
            }
            eff_params = params if not using_fallback else fallback_params
            if using_fallback:
                logger.info(f"FetchPlayers (fallback) params: {fallback_params}")
            else:
                logger.info(
                    f"FetchPlayerListing params ({'camel' if use_camelcase_params else 'snake'}): {params}"
                )
            # Log the fully prepared URL before sending
            try:
                import requests as _r
                _prep = _r.Request("GET", target_url, params=eff_params, headers=headers).prepare()
                logger.info(f"FA API prepared URL: {_prep.url}")
            except Exception:
                pass

            try:
                resp = self._request_with_retries("GET", target_url, headers=headers, timeout=20, params=eff_params)
            except Exception as e:
                # If Listing with snake_case failed, try Listing with camelCase before switching endpoints
                if not using_fallback and not use_camelcase_params:
                    logger.info(
                        f"FA Listing request failed (snake_case) for week {week} at offset {result_offset}: {e}. "
                        f"Retrying Listing with camelCase params."
                    )
                    use_camelcase_params = True
                    continue
                # If Listing with camelCase failed, try FetchPlayers fallback
                if not using_fallback and use_camelcase_params:
                    logger.info(
                        f"FA Listing request failed (camelCase) for week {week} at offset {result_offset}: {e}. "
                        f"Trying fallback FetchPlayers."
                    )
                    using_fallback = True
                    continue
                else:
                    logger.info(
                        f"FA fallback request failed for week {week} at offset {result_offset}: {e}. Aborting."
                    )
                    break
            try:
                # Log request URL as seen by server (with encoded params) and status code
                try:
                    logger.info(
                        f"FA API request -> {resp.request.method} {resp.url} (status {resp.status_code})"
                    )
                except Exception:
                    pass

                data = resp.json()
                # Briefly summarize top-level keys for visibility
                try:
                    top_keys = list(data.keys())
                    logger.info(f"FA API JSON keys: {top_keys}")
                except Exception:
                    pass
            except Exception as e:
                body_snippet = ""
                try:
                    body_snippet = resp.text[:300]
                except Exception:
                    pass
                logger.info(
                    f"Free agent response not JSON for week {week} (status {getattr(resp,'status_code',None)}): "
                    f"{body_snippet}"
                )
                break

            # Fleaflicker sometimes wraps results under different keys
            players = data.get("players") or data.get("results") or data.get("data") or []
            logger.info(
                f"Free agent page fetched for week {week} (offset {result_offset}) from {target_url}: "
                f"{len(players)} players"
            )
            if not players:
                # try camelCase then fallback endpoint once if primary yielded nothing at the first page
                if not using_fallback and not use_camelcase_params and result_offset == 0:
                    logger.info(
                        f"FetchPlayerListing (snake_case) yielded 0 players for week {week}; retrying with camelCase."
                    )
                    use_camelcase_params = True
                    continue
                if not using_fallback and use_camelcase_params and result_offset == 0:
                    logger.info(
                        f"FetchPlayerListing (camelCase) yielded 0 players for week {week}; trying fallback FetchPlayers."
                    )
                    using_fallback = True
                    continue
                logger.info(
                    f"...no Fleaflicker free agents returned for week {week} (offset {result_offset})."
                )
                break

            for p in players:
                pro = (
                    p.get("pro_player")
                    or p.get("proPlayer")
                    or p.get("proPlayerInfo")
                    or p.get("player", {}).get("pro_player")
                    or p.get("player", {}).get("proPlayer")
                    or {}
                )
                league_player = p.get("league_player") or p.get("leaguePlayer") or p

                base_player = BasePlayer()
                base_player.week_for_report = int(week)
                base_player.player_id = pro.get("id")
                base_player.full_name = pro.get("name_full") or pro.get("nameFull")
                base_player.first_name = pro.get("name_first") or pro.get("nameFirst") or base_player.full_name
                base_player.last_name = pro.get("name_last") or pro.get("nameLast")
                team_obj = pro.get("pro_team") or pro.get("proTeam") or {}
                base_player.nfl_team_abbr = (team_obj.get("abbreviation") or "").upper()
                pos_val = pro.get("position")
                if isinstance(pos_val, dict):
                    pos_val = pos_val.get("label") or pos_val.get("name")
                base_player.display_position = self.get_mapped_position(pos_val)
                base_player.primary_position = self.get_mapped_position(pos_val)
                pts = (
                    (league_player.get("viewing_actual_points", {}) or {}).get("value")
                    or (league_player.get("viewingActualPoints", {}) or {}).get("value")
                    or (p.get("viewing_actual_points", {}) or {}).get("value")
                    or 0
                )
                base_player.points = float(pts)
                base_player.projected_points = None

                eligible_positions = pro.get("position_eligibility") or pro.get("positionEligibility") or []
                for position in eligible_positions:
                    base_position = self.get_mapped_position(position)
                    base_player.eligible_positions.add(base_position)
                    for flex_position, positions in self.league.get_flex_positions_dict().items():
                        if base_position in positions:
                            base_player.eligible_positions.add(flex_position)

                # status may be mapped differently; leave None if not present
                inj = pro.get("injury") or {}
                base_player.status = inj.get("type_abbreviaition") or inj.get("typeAbbreviaition")

                if base_player.player_id:
                    free_agents[str(base_player.player_id)] = base_player
                    total_found += 1

            # Pagination: if API gives next offset, use it; else continue by fixed page_size if we received a full page
            next_offset = (
                data.get("result_offset_next")
                or data.get("resultOffsetNext")
            )
            if next_offset is not None:
                try:
                    result_offset = int(next_offset)
                except Exception:
                    break
            else:
                if len(players) < page_size:
                    break
                result_offset += page_size

        logger.info(f"Free agent retrieval complete for week {week}: total_found={total_found}")

        # If nothing found, attempt a broad listing and filter locally by missing owner
        if total_found == 0:
            try:
                probe_params = {
                    "sport": "NFL",
                    "league_id": str(self.league.league_id),
                    "sort": "SORT_SCORING_PERIOD",
                    "sort_period": str(week),
                    "result_limit": "200",
                    "result_offset": "0",
                    "sort_season": str(self.league.season),
                }
                probe_url = f"{self.base_url}/api/FetchPlayerListing"
                logger.info(f"FA probe (broad) params: {probe_params}")
                try:
                    import requests as _r
                    _prep = _r.Request("GET", probe_url, params=probe_params, headers=headers).prepare()
                    logger.info(f"FA probe prepared URL: {_prep.url}")
                except Exception:
                    pass
                probe_resp = self._request_with_retries(
                    "GET", probe_url, headers=headers, timeout=20, params=probe_params
                )
                data = probe_resp.json()
                players = data.get("players") or data.get("results") or data.get("data") or []
                logger.info(
                    f"FA probe returned {len(players)} players for week {week}; filtering for no owner"
                )
                added = 0
                for p in players:
                    league_player = p.get("league_player") or p.get("leaguePlayer") or {}
                    owner = (league_player.get("owner") or {}).get("id") if league_player else None
                    if owner:
                        continue
                    pro = (
                        p.get("pro_player")
                        or p.get("proPlayer")
                        or p.get("player", {}).get("pro_player")
                        or p.get("player", {}).get("proPlayer")
                        or {}
                    )
                    base_player = BasePlayer()
                    base_player.week_for_report = int(week)
                    base_player.player_id = pro.get("id")
                    base_player.full_name = pro.get("name_full") or pro.get("nameFull")
                    base_player.first_name = pro.get("name_first") or pro.get("nameFirst") or base_player.full_name
                    base_player.last_name = pro.get("name_last") or pro.get("nameLast")
                    team_obj = pro.get("pro_team") or pro.get("proTeam") or {}
                    base_player.nfl_team_abbr = (team_obj.get("abbreviation") or "").upper()
                    pos_val = pro.get("position")
                    if isinstance(pos_val, dict):
                        pos_val = pos_val.get("label") or pos_val.get("name")
                    base_player.display_position = self.get_mapped_position(pos_val)
                    base_player.primary_position = self.get_mapped_position(pos_val)
                    pts = (
                        (league_player.get("viewing_actual_points", {}) or {}).get("value")
                        or (league_player.get("viewingActualPoints", {}) or {}).get("value")
                        or 0
                    )
                    base_player.points = float(pts)
                    eligible_positions = pro.get("position_eligibility") or pro.get("positionEligibility") or []
                    for position in eligible_positions:
                        base_position = self.get_mapped_position(position)
                        base_player.eligible_positions.add(base_position)
                        for flex_position, positions in self.league.get_flex_positions_dict().items():
                            if base_position in positions:
                                base_player.eligible_positions.add(flex_position)
                    if base_player.player_id:
                        free_agents[str(base_player.player_id)] = base_player
                        added += 1
                logger.info(f"FA probe added {added} candidates for week {week}.")
            except Exception as e:
                logger.info(f"FA probe failed for week {week}: {e}")

        # write cache if desired
        try:
            if free_agents and self.league.save_data:
                week_dir.mkdir(parents=True, exist_ok=True)
                import json
                serializable = {
                    str(pid): {
                        "full_name": bp.full_name,
                        "first_name": bp.first_name,
                        "last_name": bp.last_name,
                        "nfl_team_abbr": bp.nfl_team_abbr,
                        "primary_position": bp.primary_position,
                        "eligible_positions": list(bp.eligible_positions),
                        "points": bp.points,
                    }
                    for pid, bp in free_agents.items()
                }
                with open(cache_path, "w", encoding="utf-8") as fh:
                    json.dump(serializable, fh, ensure_ascii=False)
                logger.info(
                    f"...retrieved {len(free_agents)} Fleaflicker free agents for week {week} and cached to {cache_path}."
                )
            elif not free_agents:
                logger.info(
                    f"...retrieved 0 Fleaflicker free agents for week {week}; skipping cache write."
                )
        except Exception as e:
            logger.debug(f"Failed to write cached free agents for week {week}: {e}")

        return free_agents
