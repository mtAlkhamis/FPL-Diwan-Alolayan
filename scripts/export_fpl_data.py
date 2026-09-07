
Export fpl data · PY
"""
FPL Weekly Recap - GitHub Actions data export
-----------------------------------------------
Same data pulled by weekly_recap.py, but written as ONE combined JSON file
instead of five CSVs, meant to be run by a GitHub Actions workflow and
committed into the repo so a static site (e.g. GitHub Pages) can fetch it
same-origin - no CORS issue, no manual re-upload each week.
 
If --end_gw is left at 0 (the default), it auto-detects which gameweek to
pull: if one is currently IN PROGRESS (deadline passed, matches not all
done), it includes that one and marks it "live_gw" in the output JSON (so
viewers know bonus points etc. may still shift for that week). Otherwise
it falls back to the latest fully FINISHED gameweek. Either way, no
gameweek number needs to be tracked by hand.
 
The output also carries "generated_at" - a UTC timestamp of when this
export ran - so the page can show "Last updated: ..." instead of
claiming to be live (it isn't - it only refreshes when someone clicks
the workflow button).
 
Usage (matches what the GitHub Actions workflow calls):
    python3 export_fpl_data.py --league_id=14514 --end_gw=0
    python3 export_fpl_data.py --league_id=14514 --end_gw=3   # force a specific gw
 
Output: data/fpl-data.json (path can be overridden with --output_path)
"""
 
import json
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
 
import pandas as pd
import requests
 
FPL_URL = "https://fantasy.premierleague.com/api/"
BOOTSTRAP_URL = FPL_URL + "bootstrap-static/"
LEAGUE_CLASSIC_URL = FPL_URL + "leagues-classic/"
LIVE_URL = FPL_URL + "event/{gw}/live/"
PICKS_URL = FPL_URL + "entry/{entry_id}/event/{gw}/picks/"
TRANSFERS_URL = FPL_URL + "entry/{entry_id}/transfers/"
HISTORY_URL = FPL_URL + "entry/{entry_id}/history/"
 
# Chips that let a manager swap most/all of their squad in a way that isn't
# a normal, considered transfer decision - free hit is temporary (the squad
# reverts automatically the following gameweek) and wildcard is a full
# rebuild. Neither should be eligible for the transfer awards.
SQUAD_OVERHAUL_CHIPS = {"wildcard", "freehit"}
 
# The FPL calls below (one per manager per gameweek for picks, one each per
# manager for transfers/history) are independent, network-bound requests -
# there's no reason to wait for one to finish before starting the next. This
# caps how many are in flight at once: high enough to meaningfully cut
# runtime on a big league, low enough not to look like abuse to FPL's public
# API (requests' default connection pool is also sized for 10).
MAX_WORKERS = 10
 
# How many times to retry a single request before giving up on it, and the
# base delay (seconds, multiplied by the attempt number) between retries.
# FPL's public API occasionally drops a connection or returns a transient
# 5xx - more noticeably now that requests fire concurrently - so one flaky
# request no longer has to fail the entire export.
REQUEST_RETRIES = 3
REQUEST_RETRY_BACKOFF = 1.5
 
 
def _get_json(session, url):
    """GET url and parse it as JSON, retrying on transient failures."""
    last_error = None
    for attempt in range(REQUEST_RETRIES):
        try:
            return session.get(url).json()
        except Exception as exc:  # noqa: BLE001 - deliberately broad: retry on anything
            last_error = exc
            if attempt < REQUEST_RETRIES - 1:
                time.sleep(REQUEST_RETRY_BACKOFF * (attempt + 1))
    raise last_error
 
 
def get_bootstrap(session):
    return _get_json(session, BOOTSTRAP_URL)
 
 
def get_player_info(bootstrap):
    position_names = {pt["id"]: pt["singular_name_short"] for pt in bootstrap["element_types"]}
    team_names = {team["id"]: team["short_name"] for team in bootstrap["teams"]}
    return {
        el["id"]: {
            "name": el["web_name"],
            "position": position_names.get(el["element_type"], "?"),
            "team": team_names.get(el["team"], "?"),
        }
        for el in bootstrap["elements"]
    }
 
 
def get_latest_finished_gw(bootstrap):
    """Highest gameweek number that FPL has marked as finished."""
    finished = [e["id"] for e in bootstrap["events"] if e.get("finished")]
    if not finished:
        raise RuntimeError("No finished gameweeks yet this season.")
    return max(finished)
 
 
def resolve_end_gw(bootstrap):
    """Decide which gameweek to pull up to when --end_gw=0 (auto).
 
    Returns (end_gw, is_live):
      - If a gameweek is currently in progress (deadline passed, matches
        not all finished yet), include it and report is_live=True - its
        numbers (especially bonus points) can still shift until FPL marks
        it finished.
      - Otherwise, fall back to the latest fully-finished gameweek,
        is_live=False.
    """
    events = bootstrap["events"]
    finished = [e["id"] for e in events if e.get("finished")]
    latest_finished = max(finished) if finished else 0
 
    current = next((e for e in events if e.get("is_current")), None)
    if current and not current.get("finished"):
        return current["id"], True
 
    if not latest_finished:
        raise RuntimeError("No finished or in-progress gameweeks yet this season.")
    return latest_finished, False
 
 
def get_league_entries(session, league_id):
    entries = []
    page = 1
    while True:
        url = (
            LEAGUE_CLASSIC_URL
            + str(league_id)
            + "/standings/?page_new_entries=1&page_standings="
            + str(page)
            + "&phase=1"
        )
        data = _get_json(session, url)
        results = data["standings"]["results"]
        if not results:
            break
        for r in results:
            entries.append(
                {"entry_id": r["entry"], "manager_name": r["player_name"], "team_name": r["entry_name"]}
            )
        if not data["standings"]["has_next"]:
            break
        page += 1
    return entries
 
 
def get_gw_live_points(session, gw):
    data = _get_json(session, LIVE_URL.format(gw=gw))
    return {el["id"]: el["stats"]["total_points"] for el in data["elements"]}
 
 
def get_entry_gw_picks(session, entry_id, gw):
    return _get_json(session, PICKS_URL.format(entry_id=entry_id, gw=gw))
 
 
def get_entry_transfers(session, entry_id):
    """Every transfer this entry has ever made, tagged with which gameweek
    ("event") it happened in. This is FPL's own transfer ledger - unlike
    diffing one gameweek's squad against the last, it does NOT record a
    free hit's temporary swap (or its automatic reversion the following
    week) as transfers, because it isn't one.
    """
    return _get_json(session, TRANSFERS_URL.format(entry_id=entry_id))
 
 
def get_entry_history(session, entry_id):
    """This entry's full season history in one call - current[] has one row
    per gameweek with the authoritative event_transfers / event_transfers_cost
    for that week (including free-hit weeks, where the picks endpoint's own
    entry_history has been seen to under-report the transfer count).
    """
    return _get_json(session, HISTORY_URL.format(entry_id=entry_id))
 
 
FPL_ENTRY_URL = "https://fantasy.premierleague.com/entry/{entry_id}/event/{gw}"
 
 
def build_manager_gameweek_rows(session, entries, start_gw, end_gw, player_info):
    rows, popularity_rows = [], []
    season_transfers = {}  # entry_id -> cumulative transfers made so far this season
    entry_ids = [entry["entry_id"] for entry in entries]
 
    # Transfers and per-gameweek history are each fetched ONCE per manager
    # (both endpoints already return full-season data) rather than diffing
    # picks between consecutive gameweeks. A picks diff can't tell a real
    # transfer apart from a free hit's temporary squad swap - which reverts
    # automatically the following gameweek - so it was reporting 11-13
    # phantom "transfers" on free-hit weeks, and again the week after when
    # the squad snaps back. FPL's own transfer ledger doesn't have that
    # problem: free hit and wildcard swaps just aren't recorded as transfers
    # there unless they actually are.
    #
    # These per-manager fetches (and the per-gameweek picks fetches below)
    # are all independent of each other, so they run through a shared thread
    # pool instead of one-request-at-a-time - the wall-clock cost here is
    # almost entirely network latency, not local computation, so this is
    # where parallelizing actually pays off. Only the fetching is
    # concurrent: every row is still built afterward in the league's own
    # entry order, single-threaded, so the output is identical to a fully
    # sequential run - just faster to produce.
    transfers_by_entry_gw = {}
    history_by_entry_gw = {}
    # entry_id -> that manager's own FPL history from EVERY completed
    # season they've ever played, straight from the same /history/ call
    # above (its "past" list, alongside the "current" list already used for
    # this season's transfer counts) - {"season_name": "2019/20",
    # "total_points": ..., "rank": ...} per season. Used later to
    # reconstruct a "what if today's league had always existed" past-seasons
    # archive with no separate manually-maintained file - see
    # build_hypothetical_history_df.
    past_seasons_by_entry = {}
 
    def fetch_ledger(entry_id):
        by_gw = {}
        for t in get_entry_transfers(session, entry_id):
            by_gw.setdefault(t["event"], []).append(t)
        history = get_entry_history(session, entry_id)
        return (
            entry_id,
            by_gw,
            {row["event"]: row for row in history.get("current", [])},
            history.get("past", []),
        )
 
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for entry_id, by_gw, history_by_gw, past_seasons in executor.map(fetch_ledger, entry_ids):
            transfers_by_entry_gw[entry_id] = by_gw
            history_by_entry_gw[entry_id] = history_by_gw
            past_seasons_by_entry[entry_id] = past_seasons
 
        for gw in range(start_gw, end_gw + 1):
            gw_player_points = get_gw_live_points(session, gw)
            captain_counts = Counter()
            owned_counts = Counter()
            num_managers_this_gw = 0
 
            # By far the biggest chunk of requests this script makes (one
            # per manager, every gameweek) - fetched concurrently, then
            # processed below in the same fixed order as before.
            picks_by_entry = dict(zip(
                entry_ids,
                executor.map(lambda entry_id, _gw=gw: get_entry_gw_picks(session, entry_id, _gw), entry_ids),
            ))
            print(f"  gw{gw}: fetched picks for {len(entry_ids)} managers")
 
            for entry in entries:
                entry_id = entry["entry_id"]
                data = picks_by_entry[entry_id]
                picks_hist = data.get("entry_history")
                if not picks_hist:
                    continue
                num_managers_this_gw += 1
 
                # event_transfers / event_transfers_cost come from the season
                # history endpoint, not the picks endpoint's own entry_history -
                # the latter has been seen to under-report the transfer count on
                # free-hit weeks specifically. Fall back to the picks endpoint's
                # value only if history doesn't have this gw yet for some reason.
                gw_hist = history_by_entry_gw.get(entry_id, {}).get(gw, picks_hist)
                num_transfers = gw_hist["event_transfers"]
                transfer_cost = gw_hist["event_transfers_cost"]
 
                picks = data.get("picks", [])
                squad_ids = {p["element"] for p in picks}
                captain_pick = next((p for p in picks if p["is_captain"]), None)
                captain_id = captain_pick["element"] if captain_pick else None
                captain_multiplier = captain_pick["multiplier"] if captain_pick else 0
                captain_raw_points = gw_player_points.get(captain_id, 0) if captain_id else 0
 
                # FPL's own "points" field on entry_history (picks endpoint) can lag
                # behind the live per-player stats endpoint - especially right after
                # a match finishes, while bonus points are still being finalized.
                # Rather than trust that separately-cached total, compute it
                # ourselves from the same live per-player data used for the squad
                # breakdown below, so the two always agree and both stay fresh.
                gw_points_computed = sum(
                    gw_player_points.get(p["element"], 0) * p["multiplier"] for p in picks
                )
                bench_points_computed = sum(
                    gw_player_points.get(p["element"], 0) for p in picks if p["multiplier"] == 0
                )
                points_delta = gw_points_computed - picks_hist["points"]
 
                season_transfers[entry_id] = season_transfers.get(entry_id, 0) + num_transfers
 
                for pid in squad_ids:
                    owned_counts[pid] += 1
                if captain_id:
                    captain_counts[captain_id] += 1
 
                # Real transfers for this gameweek, straight from FPL's ledger -
                # each record is one player out, one player in. Sorted by time
                # so the in/out arrays pair up index-for-index as the same swap
                # (not sorted by name or points - order is what makes them a
                # pair).
                gw_transfers = sorted(
                    transfers_by_entry_gw.get(entry_id, {}).get(gw, []),
                    key=lambda t: t.get("time", ""),
                )
                transferred_in_ids = [t["element_in"] for t in gw_transfers]
                transferred_out_ids = [t["element_out"] for t in gw_transfers]
 
                if len(gw_transfers) != num_transfers:
                    # Ledger and history disagree on the count - don't guess at
                    # which players were involved, just flag it and leave the
                    # transfer fields empty for this row.
                    print(
                        f"WARNING: transfer count mismatch for entry {entry_id} "
                        f"({entry['manager_name']}) gw{gw}: {len(gw_transfers)} "
                        f"transfer record(s) vs event_transfers={num_transfers}"
                    )
                    transferred_in_names = ""
                    transferred_out_names = ""
                    points_in = None
                    points_out = None
                    net_transfer_impact = None
                    transfer_detail_in = []
                    transfer_detail_out = []
                else:
                    transferred_in_names = ", ".join(
                        player_info.get(pid, {}).get("name", "Unknown") for pid in transferred_in_ids
                    )
                    transferred_out_names = ", ".join(
                        player_info.get(pid, {}).get("name", "Unknown") for pid in transferred_out_ids
                    )
                    # Raw points, no captain multiplier - these are the players'
                    # own gameweek scores, not what they'd have contributed to
                    # this manager's squad.
                    points_in = sum(gw_player_points.get(pid, 0) for pid in transferred_in_ids)
                    points_out = sum(gw_player_points.get(pid, 0) for pid in transferred_out_ids)
                    net_transfer_impact = points_in - points_out - transfer_cost
                    # Per-player detail behind the side totals above, same order
                    # as transferred_in_ids/transferred_out_ids (index i of one
                    # is the same swap as index i of the other).
                    transfer_detail_in = [
                        {"name": player_info.get(pid, {}).get("name", "Unknown"), "points": gw_player_points.get(pid, 0)}
                        for pid in transferred_in_ids
                    ]
                    transfer_detail_out = [
                        {"name": player_info.get(pid, {}).get("name", "Unknown"), "points": gw_player_points.get(pid, 0)}
                        for pid in transferred_out_ids
                    ]
 
                rows.append(
                    {
                        "gameweek": gw,
                        "entry_id": entry_id,
                        "manager_name": entry["manager_name"],
                        "team_name": entry["team_name"],
                        "gw_points": gw_points_computed,
                        "cumulative_points": picks_hist["total_points"] + points_delta,
                        "bench_points": bench_points_computed,
                        "team_value": picks_hist["value"] / 10,
                        "bank": picks_hist["bank"] / 10,
                        "overall_rank": picks_hist["overall_rank"],
                        "num_transfers": num_transfers,
                        "transfer_cost": transfer_cost,
                        "net_transfer_impact": net_transfer_impact,
                        "transferred_in": transferred_in_names,
                        "transferred_out": transferred_out_names,
                        "transfer_points_in": points_in,
                        "transfer_points_out": points_out,
                        # Per-player detail behind transfer_points_in/out - kept
                        # off manager_gameweek_stats (dropped before that dataset
                        # is written out below) and used only to build the
                        # per-player arrays on the transfer awards in
                        # gameweek_highlights.
                        "transfer_detail_in": transfer_detail_in,
                        "transfer_detail_out": transfer_detail_out,
                        "season_transfers_to_date": season_transfers[entry_id],
                        "chip_played": data.get("active_chip"),
                        "captain_id": captain_id,
                        "captain_name": player_info.get(captain_id, {}).get("name", "Unknown"),
                        "captain_multiplier": captain_multiplier,
                        "captain_contribution": captain_raw_points * captain_multiplier,
                        # Full per-player squad detail used to be included here as its
                        # own dataset (every player, every manager, every gameweek) -
                        # that's the bulk of the file's size and it only grows every
                        # week. A link to the manager's own team page on FPL's site
                        # covers the same "who did they play" need on demand, without
                        # carrying the data ourselves.
                        "team_link": FPL_ENTRY_URL.format(entry_id=entry_id, gw=gw),
                    }
                )
 
            for pid, owned_count in owned_counts.items():
                popularity_rows.append(
                    {
                        "gameweek": gw,
                        "player_id": pid,
                        "player_name": player_info.get(pid, {}).get("name", "Unknown"),
                        "times_owned": owned_count,
                        "pct_owned": round(100 * owned_count / num_managers_this_gw, 1)
                        if num_managers_this_gw
                        else 0,
                        "times_captained": captain_counts.get(pid, 0),
                        "pct_captained": round(100 * captain_counts.get(pid, 0) / num_managers_this_gw, 1)
                        if num_managers_this_gw
                        else 0,
                    }
                )
 
    return rows, popularity_rows, past_seasons_by_entry
 
 
def add_league_rank_and_movement(df):
    df = df.copy()
    df["league_rank"] = df.groupby("gameweek")["cumulative_points"].rank(
        ascending=False, method="min"
    ).astype(int)
    df = df.sort_values(["entry_id", "gameweek"])
    df["prev_rank"] = df.groupby("entry_id")["league_rank"].shift(1)
    df["rank_change"] = df["prev_rank"] - df["league_rank"]
    df = df.drop(columns=["prev_rank"])
    return df.sort_values(["gameweek", "league_rank"]).reset_index(drop=True)
 
 
def build_gameweek_highlights(df, popularity_df):
    first_gw = df["gameweek"].min()
    highlight_rows = []
    for gw, gw_df in df.groupby("gameweek"):
        if gw == first_gw:
            # Everyone's squad is brand new on the very first tracked
            # gameweek - nobody's had a chance to be a "ghost" yet, so
            # nobody is excluded here.
            active_df = gw_df
        else:
            # A "ghost" - a manager who has made zero transfers all season -
            # is still running whatever squad they set on gw1. That can go
            # stale in ways that quietly "win" awards they shouldn't: e.g. a
            # captain who's since left the Premier League entirely and now
            # scores 0 forever, making them a lock for "worst captain" every
            # week. Exclude ghosts from every comparison below, not just the
            # transfer ones.
            active_df = gw_df[gw_df["season_transfers_to_date"] > 0]
            if active_df.empty:  # everyone happens to be a ghost - don't blank the gw
                active_df = gw_df
 
        top = active_df.loc[active_df["gw_points"].idxmax()]
        bottom = active_df.loc[active_df["gw_points"].idxmin()]
 
        movers = active_df.dropna(subset=["rank_change"])
        riser = movers.loc[movers["rank_change"].idxmax()] if not movers.empty else None
        faller = movers.loc[movers["rank_change"].idxmin()] if not movers.empty else None
 
        best_cap = active_df.loc[active_df["captain_contribution"].idxmax()]
        worst_cap = active_df.loc[active_df["captain_contribution"].idxmin()]
        most_wasted = active_df.loc[active_df["bench_points"].idxmax()]
 
        value_king = active_df.loc[active_df["team_value"].idxmax()]
        value_laggard = active_df.loc[active_df["team_value"].idxmin()]
 
        chip_rows = active_df[active_df["chip_played"].notna()]
        chip_master = chip_rows.loc[chip_rows["gw_points"].idxmax()] if not chip_rows.empty else None
        no_chip_rows = active_df[active_df["chip_played"].isna()]
        no_chip_warrior = (
            no_chip_rows.loc[no_chip_rows["gw_points"].idxmax()] if not no_chip_rows.empty else None
        )
 
        # On top of the season-long ghost exclusion above, only managers who
        # actually made a transfer THIS gameweek are eligible for the
        # transfer awards - an otherwise-active manager who simply didn't
        # transfer this particular week still nets exactly 0, which isn't a
        # "transfer" to award. Free hit and wildcard weeks are excluded too:
        # both let a manager swap most/all of their squad at once, which
        # isn't a "best transfer" in the sense these awards mean, and free
        # hit's automatic reversion the following week is exactly what used
        # to produce a phantom 11-13 name transfer list under this label.
        transfer_rows = active_df.dropna(subset=["net_transfer_impact"])
        transfer_rows = transfer_rows[transfer_rows["num_transfers"] > 0]
        transfer_rows = transfer_rows[~transfer_rows["chip_played"].isin(SQUAD_OVERHAUL_CHIPS)]
        if not transfer_rows.empty:
            sharpest_trader = transfer_rows.loc[transfer_rows["net_transfer_impact"].idxmax()]
            transfer_tangle = transfer_rows.loc[transfer_rows["net_transfer_impact"].idxmin()]
        else:
            sharpest_trader = transfer_tangle = None
 
        # "Best single transfer" / "worst single transfer" - the single best
        # (and worst) individual swap ANYWHERE in the league that gameweek,
        # not aggregated per manager. A manager who made several transfers
        # is still eligible via whichever one of their own swaps was best
        # (or worst) - unlike the old rule, they're not excluded just for
        # having made more than one move. Uses the same population as
        # sharpest_trader/transfer_tangle above (transfer_rows already has
        # season-long ghosts and wildcard/free-hit weeks excluded), just
        # scored per individual swap rather than per manager's net total for
        # the gameweek. No transfer-cost deduction here: cost is charged
        # once for the whole gameweek, not attributable to one swap out of
        # possibly several, so this is a plain points_in - points_out.
        best_swap = None
        worst_swap = None
        for _, row in transfer_rows.iterrows():
            for pin, pout in zip(row["transfer_detail_in"], row["transfer_detail_out"]):
                swap = {
                    "manager_name": row["manager_name"],
                    "player_in": pin["name"],
                    "points_in": pin["points"],
                    "player_out": pout["name"],
                    "points_out": pout["points"],
                    "net_impact": pin["points"] - pout["points"],
                }
                if best_swap is None or swap["net_impact"] > best_swap["net_impact"]:
                    best_swap = swap
                if worst_swap is None or swap["net_impact"] < worst_swap["net_impact"]:
                    worst_swap = swap
 
        # Sanity check the per-player detail behind sharpest_trader/
        # transfer_tangle before it goes anywhere near the JSON: array
        # lengths must match num_transfers, and the per-player points must
        # sum to the side totals already computed above. These should
        # always hold by construction (both come from the same
        # transfer_detail_in/out built alongside transfer_points_in/out) -
        # fail loudly rather than silently write a file where a
        # page-rendered breakdown wouldn't add up to its own total.
        # (one_move_master/one_move_disaster are built directly from the
        # same per-player dicts above, so there's nothing separate to
        # cross-check for them.)
        for award_label, award_row in (
            ("sharpest_trader", sharpest_trader),
            ("transfer_tangle", transfer_tangle),
        ):
            if award_row is None:
                continue
            n_in = len(award_row["transfer_detail_in"])
            n_out = len(award_row["transfer_detail_out"])
            assert n_in == n_out == award_row["num_transfers"], (
                f"{award_label} gw{gw} ({award_row['manager_name']}): array "
                f"lengths in={n_in} out={n_out} vs num_transfers="
                f"{award_row['num_transfers']}"
            )
            assert sum(p["points"] for p in award_row["transfer_detail_in"]) == award_row["transfer_points_in"], (
                f"{award_label} gw{gw} ({award_row['manager_name']}): per-player "
                f"in-points don't sum to transfer_points_in"
            )
            assert sum(p["points"] for p in award_row["transfer_detail_out"]) == award_row["transfer_points_out"], (
                f"{award_label} gw{gw} ({award_row['manager_name']}): per-player "
                f"out-points don't sum to transfer_points_out"
            )
 
        chips_this_gw = gw_df[gw_df["chip_played"].notna()][["manager_name", "chip_played"]].to_dict(
            "records"
        )
 
        pop_gw = popularity_df[popularity_df["gameweek"] == gw]
        most_captained = pop_gw.loc[pop_gw["pct_captained"].idxmax()] if not pop_gw.empty else None
        most_owned = pop_gw.loc[pop_gw["pct_owned"].idxmax()] if not pop_gw.empty else None
 
        def name_or_blank(row, col="manager_name"):
            return row[col] if row is not None else ""
 
        highlight_rows.append(
            {
                "gameweek": gw,
                "top_scorer": top["manager_name"],
                "top_score": top["gw_points"],
                "bottom_scorer": bottom["manager_name"],
                "bottom_score": bottom["gw_points"],
                "biggest_riser": name_or_blank(riser),
                "riser_places_gained": riser["rank_change"] if riser is not None else None,
                "biggest_faller": name_or_blank(faller),
                "faller_places_lost": abs(faller["rank_change"]) if faller is not None else None,
                "best_captain_manager": best_cap["manager_name"],
                "best_captain_player": best_cap["captain_name"],
                "best_captain_points": best_cap["captain_contribution"],
                "worst_captain_manager": worst_cap["manager_name"],
                "worst_captain_player": worst_cap["captain_name"],
                "worst_captain_points": worst_cap["captain_contribution"],
                "most_wasted_bench_manager": most_wasted["manager_name"],
                "wasted_bench_points": most_wasted["bench_points"],
                "value_king_manager": value_king["manager_name"],
                "value_king_value": value_king["team_value"],
                "value_laggard_manager": value_laggard["manager_name"],
                "value_laggard_value": value_laggard["team_value"],
                "chip_master_manager": name_or_blank(chip_master),
                "chip_master_chip": chip_master["chip_played"] if chip_master is not None else "",
                "chip_master_score": chip_master["gw_points"] if chip_master is not None else None,
                "no_chip_warrior_manager": name_or_blank(no_chip_warrior),
                "no_chip_warrior_score": no_chip_warrior["gw_points"]
                if no_chip_warrior is not None
                else None,
                # "Total best transfer" - best net impact regardless of how many
                # moves it took; players_in/out list everyone who moved that gw.
                "sharpest_trader_manager": name_or_blank(sharpest_trader),
                "sharpest_trader_net_impact": sharpest_trader["net_transfer_impact"]
                if sharpest_trader is not None
                else None,
                "sharpest_trader_players_in": sharpest_trader["transferred_in"]
                if sharpest_trader is not None
                else "",
                "sharpest_trader_players_out": sharpest_trader["transferred_out"]
                if sharpest_trader is not None
                else "",
                "sharpest_trader_points_in": sharpest_trader["transfer_points_in"]
                if sharpest_trader is not None
                else None,
                "sharpest_trader_points_out": sharpest_trader["transfer_points_out"]
                if sharpest_trader is not None
                else None,
                # Per-player breakdown behind the totals above - out[i]/in[i]
                # is the same swap, in the order the transfers were made.
                "sharpest_trader_out": sharpest_trader["transfer_detail_out"]
                if sharpest_trader is not None
                else [],
                "sharpest_trader_in": sharpest_trader["transfer_detail_in"]
                if sharpest_trader is not None
                else [],
                "transfer_tangle_manager": name_or_blank(transfer_tangle),
                "transfer_tangle_net_impact": transfer_tangle["net_transfer_impact"]
                if transfer_tangle is not None
                else None,
                "transfer_tangle_players_in": transfer_tangle["transferred_in"]
                if transfer_tangle is not None
                else "",
                "transfer_tangle_players_out": transfer_tangle["transferred_out"]
                if transfer_tangle is not None
                else "",
                "transfer_tangle_points_in": transfer_tangle["transfer_points_in"]
                if transfer_tangle is not None
                else None,
                "transfer_tangle_points_out": transfer_tangle["transfer_points_out"]
                if transfer_tangle is not None
                else None,
                "transfer_tangle_out": transfer_tangle["transfer_detail_out"]
                if transfer_tangle is not None
                else [],
                "transfer_tangle_in": transfer_tangle["transfer_detail_in"]
                if transfer_tangle is not None
                else [],
                # "Best single transfer" / "worst single transfer" - the one
                # best (and worst) individual swap in the whole league this
                # gameweek, win from best_swap/worst_swap above.
                "one_move_master_manager": best_swap["manager_name"] if best_swap else "",
                "one_move_master_net_impact": best_swap["net_impact"] if best_swap else None,
                "one_move_master_player_in": best_swap["player_in"] if best_swap else "",
                "one_move_master_player_out": best_swap["player_out"] if best_swap else "",
                "one_move_master_points_in": best_swap["points_in"] if best_swap else None,
                "one_move_master_points_out": best_swap["points_out"] if best_swap else None,
                "one_move_master_out": [
                    {"name": best_swap["player_out"], "points": best_swap["points_out"]}
                ] if best_swap else [],
                "one_move_master_in": [
                    {"name": best_swap["player_in"], "points": best_swap["points_in"]}
                ] if best_swap else [],
                "one_move_disaster_manager": worst_swap["manager_name"] if worst_swap else "",
                "one_move_disaster_net_impact": worst_swap["net_impact"] if worst_swap else None,
                "one_move_disaster_player_in": worst_swap["player_in"] if worst_swap else "",
                "one_move_disaster_player_out": worst_swap["player_out"] if worst_swap else "",
                "one_move_disaster_points_in": worst_swap["points_in"] if worst_swap else None,
                "one_move_disaster_points_out": worst_swap["points_out"] if worst_swap else None,
                "one_move_disaster_out": [
                    {"name": worst_swap["player_out"], "points": worst_swap["points_out"]}
                ] if worst_swap else [],
                "one_move_disaster_in": [
                    {"name": worst_swap["player_in"], "points": worst_swap["points_in"]}
                ] if worst_swap else [],
                "most_captained_player": most_captained["player_name"] if most_captained is not None else "",
                "most_captained_pct": most_captained["pct_captained"] if most_captained is not None else None,
                "most_owned_player": most_owned["player_name"] if most_owned is not None else "",
                "most_owned_pct": most_owned["pct_owned"] if most_owned is not None else None,
                "chips_played": "; ".join(
                    f"{c['manager_name']} ({c['chip_played']})" for c in chips_this_gw
                ),
            }
        )
    return pd.DataFrame(highlight_rows)
 
 
def build_season_summary(df):
    latest_gw = df["gameweek"].max()
    latest = df[df["gameweek"] == latest_gw].set_index("entry_id")
 
    summary = df.groupby(["entry_id", "manager_name", "team_name"]).agg(
        total_transfers=("num_transfers", "sum"),
        total_transfer_cost=("transfer_cost", "sum"),
        best_overall_rank=("overall_rank", "min"),
        gameweeks_tracked=("gameweek", "count"),
    ).reset_index()
 
    summary["current_team_value"] = summary["entry_id"].map(latest["team_value"])
    summary["current_cumulative_points"] = summary["entry_id"].map(latest["cumulative_points"])
    summary["current_league_rank"] = summary["entry_id"].map(latest["league_rank"])
    return summary.sort_values("current_league_rank").reset_index(drop=True)
 
 
def df_records(df):
    """Convert a DataFrame to plain JSON-safe records (NaN -> null)."""
    return json.loads(df.to_json(orient="records"))
 
 
def build_hypothetical_history_df(entries, past_seasons_by_entry):
    """Reconstruct a "what if today's league had always existed" past-seasons
    table, straight from FPL's own API - no manually-maintained file needed.
 
    FPL doesn't track who was actually IN a given mini-league for a season
    that's already rolled over - that's why the CSV-based path below
    exists. But it DOES track, for every individual manager, their own
    season-by-season total points going back to whenever they started
    playing FPL (get_entry_history's "past" list, already fetched once per
    manager in build_manager_gameweek_rows - see past_seasons_by_entry).
    So instead of the real historical standings, this ranks TODAY's league
    members against each other using their own historical totals for each
    season: same shape of output (champion/runner-up/etc per season), but
    "champion" means "highest-scoring of today's members that season", not
    necessarily who actually won any real league back then. Two honest
    caveats that follow from that: a manager who has since left this league
    won't appear in ANY season here, even ones they actually won; and team
    names shown are each manager's CURRENT team name, since FPL doesn't
    retain what a team was called in a past season.
 
    Returns an empty-shaped DataFrame (not None) when nobody has any past
    seasons on record - e.g. a brand-new league whose members are all new
    to FPL too - so build_season_history's "nothing completed yet" branch
    handles it the same way as an empty CSV would.
    """
    entry_lookup = {entry["entry_id"]: entry for entry in entries}
    rows = []
    for entry_id, past_seasons in past_seasons_by_entry.items():
        entry = entry_lookup.get(entry_id)
        if entry is None:
            continue
        for season in past_seasons:
            rows.append(
                {
                    "Season": season["season_name"],
                    "Manager": entry["manager_name"],
                    "Team": entry["team_name"],
                    "Total Points": season["total_points"],
                    "Overall Rank": season["rank"],
                }
            )
 
    if not rows:
        return pd.DataFrame(columns=["Season", "Pos", "Manager", "Team", "Total Points", "Overall Rank"])
 
    df = pd.DataFrame(rows)
    # "Pos" here is OUR OWN ranking of today's members against each other
    # within each season - FPL doesn't provide one, since this grouping
    # never existed as a real league back then.
    df["Pos"] = (
        df.groupby("Season")["Total Points"].rank(ascending=False, method="min").astype(int)
    )
    return df
 
 
def load_season_history(path):
    """Read a manually-maintained past-seasons file (columns: Season, Pos,
    Manager, Team, Total Points, Overall Rank), for callers who want the
    REAL historical league standings instead of the live-reconstructed
    hypothetical table above (build_hypothetical_history_df) - e.g. to
    preserve a real former champion who has since left the league. Fully
    optional: main() only uses this when a season_history_path is
    explicitly given and the file exists; otherwise the hypothetical,
    API-only reconstruction is what actually ships.
 
    Returns None if the file doesn't exist.
    """
    path = Path(path)
    if not path.exists():
        return None
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df
 
 
def build_season_history(history_df, exclude_current_season=True):
    """Turn a long-format past-seasons table (Season, Pos, Manager, Team,
    Total Points, Overall Rank - either the manually-maintained CSV via
    load_season_history, or the live reconstruction from
    build_hypothetical_history_df) into an archive: a
    champion/runner-up/third/wooden-spoon card for each completed season, a
    career "hall of fame" per manager, and two all-time record cards.
 
    exclude_current_season controls whether the most recent season present
    is treated as still in progress and dropped from every stat here.
    Needed for the manually-maintained CSV, which typically DOES include an
    in-progress season as its latest row (and that season is already
    covered live by season_summary/gameweek_highlights elsewhere, so
    showing it here too would present an unfinished season as final).
    Pass False for the live API reconstruction, whose input never includes
    an in-progress season in the first place (FPL's own "past" history
    only ever lists fully completed seasons) - there, current_season comes
    back None since there's nothing to exclude.
    """
    if history_df is None or history_df.empty:
        return None
 
    df = history_df.copy()
    if exclude_current_season:
        current_season = df["Season"].max()  # "YYYY/YY" strings sort correctly
        past_df = df[df["Season"] != current_season]
    else:
        current_season = None
        past_df = df
 
    def _entry(row):
        if row is None:
            return None
        return {
            "manager_name": row["Manager"],
            "team_name": row["Team"],
            "points": int(row["Total Points"]),
        }
 
    past_seasons = []
    for season, season_df in past_df.groupby("Season"):
        season_df = season_df.sort_values("Pos")
        champion = season_df.iloc[0]
        runner_up = season_df.iloc[1] if len(season_df) > 1 else None
        third_place = season_df.iloc[2] if len(season_df) > 2 else None
        wooden_spoon = season_df.iloc[-1]
 
        past_seasons.append(
            {
                "season": season,
                "num_managers": len(season_df),
                "champion": _entry(champion),
                "runner_up": _entry(runner_up),
                "third_place": _entry(third_place),
                "wooden_spoon": _entry(wooden_spoon),
            }
        )
    # Most recent completed season first - how a "past champions" list is
    # usually read.
    past_seasons.sort(key=lambda s: s["season"], reverse=True)
 
    if past_df.empty:
        # A brand-new league (like a mini-league whose very first season is
        # still in progress) has nothing completed yet - an empty archive,
        # not an error.
        return {
            "current_season": current_season,
            "seasons_completed": 0,
            "past_seasons": [],
            "hall_of_fame": [],
            "highest_single_season_score": None,
            "most_improved_season": None,
        }
 
    # Hall of fame - one row per manager, career totals across every
    # completed season they appear in (not necessarily every season the
    # league has run, if they joined partway through).
    hall_of_fame = []
    for manager, mgr_df in past_df.groupby("Manager"):
        best_row = mgr_df.loc[mgr_df["Pos"].idxmin()]
        hall_of_fame.append(
            {
                "manager_name": manager,
                "seasons_played": len(mgr_df),
                "titles": int((mgr_df["Pos"] == 1).sum()),
                "runner_up_finishes": int((mgr_df["Pos"] == 2).sum()),
                "top_3_finishes": int((mgr_df["Pos"] <= 3).sum()),
                "best_finish": int(best_row["Pos"]),
                "best_finish_season": best_row["Season"],
                "career_total_points": int(mgr_df["Total Points"].sum()),
                "average_points": round(float(mgr_df["Total Points"].mean()), 1),
            }
        )
    hall_of_fame.sort(
        key=lambda r: (-r["titles"], -r["top_3_finishes"], -r["career_total_points"])
    )
 
    best_season_row = past_df.loc[past_df["Total Points"].idxmax()]
    highest_single_season_score = {
        "manager_name": best_season_row["Manager"],
        "team_name": best_season_row["Team"],
        "season": best_season_row["Season"],
        "points": int(best_season_row["Total Points"]),
    }
 
    # Biggest single-season jump in league position for one manager, between
    # the two seasons they most recently appear in back-to-back in THEIR
    # OWN history (not necessarily adjacent calendar seasons, if they sat a
    # year out at some point).
    most_improved_season = None
    best_jump = None
    for manager, mgr_df in past_df.groupby("Manager"):
        mgr_df = mgr_df.sort_values("Season")
        seasons = mgr_df["Season"].tolist()
        positions = mgr_df["Pos"].tolist()
        for i in range(1, len(seasons)):
            jump = positions[i - 1] - positions[i]  # positive = moved UP the table
            if best_jump is None or jump > best_jump:
                best_jump = jump
                most_improved_season = {
                    "manager_name": manager,
                    "from_season": seasons[i - 1],
                    "to_season": seasons[i],
                    "from_position": int(positions[i - 1]),
                    "to_position": int(positions[i]),
                    "places_gained": int(jump),
                }
 
    return {
        "current_season": current_season,
        "seasons_completed": len(past_seasons),
        "past_seasons": past_seasons,
        "hall_of_fame": hall_of_fame,
        "highest_single_season_score": highest_single_season_score,
        "most_improved_season": most_improved_season,
    }
 
 
def validate_transfer_consistency(df):
    """Final sanity pass before writing the JSON out - catches an entire bug
    class rather than relying on individual bad rows being noticed later.
    Logs any issue found; never raises, so one manager's bad data doesn't
    block the whole weekly export from running.
    """
    issues = []
 
    for _, row in df.iterrows():
        in_names = [n for n in row["transferred_in"].split(", ") if n]
        out_names = [n for n in row["transferred_out"].split(", ") if n]
        # A mismatch row deliberately has both fields blank (see the
        # WARNING logged where it's built) - nothing to check there.
        if not in_names and not out_names:
            continue
        if len(in_names) != len(out_names) or len(in_names) != row["num_transfers"]:
            issues.append(
                f"entry {row['entry_id']} ({row['manager_name']}) gw{row['gameweek']}: "
                f"in={len(in_names)} out={len(out_names)} num_transfers={row['num_transfers']}"
            )
 
    prev_by_entry = {}
    for _, row in df.sort_values(["entry_id", "gameweek"]).iterrows():
        entry_id = row["entry_id"]
        prev = prev_by_entry.get(entry_id)
        if prev is not None and row["season_transfers_to_date"] < prev:
            issues.append(
                f"entry {entry_id} ({row['manager_name']}): season_transfers_to_date "
                f"dropped from {prev} to {row['season_transfers_to_date']} at gw{row['gameweek']}"
            )
        prev_by_entry[entry_id] = row["season_transfers_to_date"]
 
    if issues:
        print(f"Transfer consistency check found {len(issues)} issue(s):")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("Transfer consistency check passed - all rows agree.")
    return issues
 
 
def main(
    league_id=14514,
    start_gw=1,
    end_gw=0,
    output_path="data/fpl-data.json",
    season_history_path=None,
):
    started_at = time.perf_counter()
    session = requests.session()
 
    print(f"Fetching player info and league '{league_id}' entries...")
    bootstrap = get_bootstrap(session)
    player_info = get_player_info(bootstrap)
    entries = get_league_entries(session, league_id)
    print(f"Found {len(entries)} managers in the league.")
 
    is_live = False
    if not end_gw:
        end_gw, is_live = resolve_end_gw(bootstrap)
        status = "IN PROGRESS - numbers may still shift" if is_live else "finished"
        print(f"Auto-detected gameweek {end_gw} ({status})")
 
    print(f"Pulling gameweeks {start_gw}-{end_gw} (this can take a minute)...")
    rows, popularity_rows, past_seasons_by_entry = build_manager_gameweek_rows(
        session, entries, start_gw, end_gw, player_info
    )
    df = pd.DataFrame(rows)
    df = add_league_rank_and_movement(df)
    validate_transfer_consistency(df)
    popularity_df = pd.DataFrame(popularity_rows)
    # highlights and season_summary are built from the full df (they need
    # transfer_detail_in/out to populate the per-player transfer arrays);
    # manager_gameweek_stats itself doesn't carry that detail - it's kept
    # scoped to gameweek_highlights only.
    highlights_df = build_gameweek_highlights(df, popularity_df)
    season_df = build_season_summary(df)
    stats_df = df.drop(columns=["transfer_detail_in", "transfer_detail_out"])
 
    # Past-seasons archive (champions, hall of fame, all-time records).
    # Default: reconstructed entirely from FPL's own API - today's members
    # ranked against each other by their own historical season totals, no
    # extra file to maintain (see build_hypothetical_history_df for what
    # that means and its caveats). Passing season_history_path points this
    # at a manually-maintained CSV instead, for the real historical league
    # standings rather than the hypothetical reconstruction - see
    # load_season_history.
    if season_history_path and Path(season_history_path).exists():
        season_history_df = load_season_history(season_history_path)
        season_history = build_season_history(season_history_df, exclude_current_season=True)
        print(f"Loaded manually-maintained season history from {season_history_path}.")
    else:
        season_history_df = build_hypothetical_history_df(entries, past_seasons_by_entry)
        season_history = build_season_history(season_history_df, exclude_current_season=False)
 
    if season_history is None:
        print("No past-season history available - skipping season_history.")
    else:
        print(
            f"season_history: {season_history['seasons_completed']} completed "
            "season(s) covered."
        )
 
    combined = {
        "league_id": league_id,
        "start_gw": start_gw,
        "end_gw": end_gw,
        "live_gw": end_gw if is_live else None,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "manager_gameweek_stats": df_records(stats_df),
        "player_gameweek_popularity": df_records(popularity_df),
        "gameweek_highlights": df_records(highlights_df),
        "season_summary": df_records(season_df),
        "season_history": season_history,
    }
 
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(combined, f, indent=2)
    print(f"Wrote {out_path} in {time.perf_counter() - started_at:.1f}s")
 
 
if __name__ == "__main__":
    import fire
 
    fire.Fire(main)
 


Couldn't save export_fpl_data.py.
