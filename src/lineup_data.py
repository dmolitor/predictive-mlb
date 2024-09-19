from daily_lineups import extract_lineups, lineups_by_date, previous_day, today
import pandas as pd
from pathlib import Path
import pins
from pybaseball import (
    playerid_reverse_lookup,
    team_game_logs
)
import pybaseball.league_batting_stats as lbs
import pybaseball.league_pitching_stats as lps
import time
from tqdm import tqdm

### Import proxies

"""
Baseball Reference is being stupid and blocking me for crawling (super slowly!!!)
This cycles through 10 proxies and allows me to crawl faster without getting blocked.
"""

import os
import requests
 
response = requests.get(
    "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=25",
    headers={"Authorization": os.environ["WEBSHARE_TOKEN"]}
)
 
proxy_list = response.json()["results"]
current_proxy_index = 0

lbs.session.max_requests_per_minute = 20
lps.session.max_requests_per_minute = 20

##################

all_star_game = {
    2024: "2024-07-16"
}
opening_day = {
    2024: "2024-03-28"
}

bref_teams = {
    "angels": "LAA",
    "d-backs": "ARI",
    "braves": "ATL",
    "orioles": "BAL",
    "red sox": "BOS",
    "cubs": "CHC",
    "white sox": "CHW",
    "reds": "CIN",
    "guardians": "CLE",
    "rockies": "COL",
    "tigers": "DET",
    "marlins": "MIA",
    "astros": "HOU",
    "royals": "KCR",
    "dodgers": "LAD",
    "brewers": "MIL",
    "twins": "MIN",
    "mets": "NYM",
    "yankees": "NYY",
    "athletics": "OAK",
    "phillies": "PHI",
    "pirates": "PIT",
    "padres": "SDP",
    "mariners": "SEA",
    "giants": "SFG",
    "cardinals": "STL",
    "rays": "TBR",
    "rangers": "TEX",
    "blue jays": "TOR",
    "nationals": "WSN"
}

def batting_stats(ids: list, stats: pd.DataFrame) -> pd.DataFrame:
    """
    Extract and normalize batting stats for given player IDs.
    """
    keep_cols = [
        "R", "H", "2B", "3B", "HR", "RBI", "BB", "IBB", "SO",
        "HBP", "SH", "SF", "GDP", "SB", "CS", "BA", "OBP", "SLG", "OPS"
    ]
    normalize_cols = [
        "R", "H", "2B", "3B", "HR", "RBI", "BB", "IBB", "SO",
        "HBP", "SH", "SF", "GDP", "SB", "CS"
    ]
    if stats.empty:
        return pd.DataFrame(columns=keep_cols)
    id_df = pd.DataFrame({"mlb_id": ids})
    stats["mlbID"] = stats["mlbID"].astype(int)
    stats = pd.merge(
        id_df, stats, left_on="mlb_id", right_on="mlbID", how="left"
    )
    for col in normalize_cols:
        stats[col] = stats[col] / stats["PA"]
    return stats[keep_cols]

def game_logs(date: str = today()):
    logs = []
    for team in tqdm(bref_teams.keys(), total=len(bref_teams.keys())):
        team_logs = game_score(date=date, team=team)
        team_logs["team"] = [team]*len(team_logs)
        logs.append(team_logs)
    logs = pd.concat(logs, axis=0).reset_index(drop=True)
    return logs

def game_score(date: str, team: str) -> pd.DataFrame:
    """
    Retrieve game logs for a specific team up to a given date.
    """
    date = pd.to_datetime(date)
    year = date.year
    team = bref_teams[team.lower()]
    game_logs = team_game_logs(season=year, team=team)
    game_logs[["clean_date", "game_date_id"]] = (
        game_logs["Date"]
        .str
        .extract(r"([A-Za-z]{3} \d{1,2})\s*(\(\d\))?")
    )
    game_logs["game_date_id"] = (
        game_logs["game_date_id"]
        .str
        .extract(r"\((\d+)\)")
        .fillna(1)
    )
    game_logs["clean_date"] = pd.to_datetime(
        f"{str(year)} " + game_logs["clean_date"],
        format="mixed"
    )
    game_logs.columns = game_logs.columns.str.lower()
    return game_logs[game_logs["clean_date"] < date]

def id_reverse_lookup(ids: list) -> pd.DataFrame:
    """
    Perform reverse lookup for player IDs and sort the result.
    """
    data = playerid_reverse_lookup(ids, key_type="mlbam")
    data["key_mlbam"] = pd.Categorical(
        data["key_mlbam"],
        categories=ids,
        ordered=True
    )
    data.sort_values("key_mlbam", inplace=True)
    return data

def lineups(date: str = today()) -> pd.DataFrame:
    """
    Retrieve and process lineups for a given date.
    """
    year = pd.to_datetime(date).year
    starting_lineups = []
    lineups = lineups_by_date(date=date)
    if previous_day(opening_day[year]) == previous_day(date):
        batting_data = pd.DataFrame()
        pitching_data = pd.DataFrame()
    else:
        global current_proxy_index
        current_proxy = proxy(proxy_list[current_proxy_index])
        lbs.session.session.proxies = {"http": current_proxy, "https": current_proxy}
        lps.session.session.proxies = {"http": current_proxy, "https": current_proxy}
        current_proxy_index = (current_proxy_index + 1) % len(proxy_list)
        batting_data = lbs.batting_stats_range(
            start_dt=opening_day[year],
            end_dt=previous_day(date)
        )
        pitching_data = lps.pitching_stats_range(
            start_dt=opening_day[year],
            end_dt=previous_day(date)
        )
    for matchup in lineups.lineups:
        try:
            starting_lineup = extract_lineups(matchup, lineups.date)
            if starting_lineup is not None:
                starting_lineups.append(starting_lineup)
        except:
            continue
    matchups = []
    lineups = []
    for lineup in starting_lineups:
        matchup = lineup["home"]["team"] + "@" + lineup["away"]["team"]
        if matchup in matchups:
            game_id = 2
        else:
            game_id = 1
        matchups.append(matchup)
        lineup_predictors = lineup_to_predictors(lineup, batting_data, pitching_data)
        lineup_predictors["game_date_id"] = game_id
        lineups.append(lineup_predictors)
    if lineups:
        out = pd.concat(lineups, axis=0).reset_index(drop=True)
    else:
        out = None
    return out

def lineups_up_to_date(date: str = today(), verbose: bool = True) -> pd.DataFrame:
    """
    Retrieve lineups for all dates from opening day to the specified date.
    """
    date = pd.to_datetime(date)
    year = date.year
    lineups_list = []
    date_range = pd.date_range(start=opening_day[year], end=date, inclusive="left")
    all_star_date = pd.to_datetime(all_star_game[year])
    if all_star_date in date_range:
        date_range = date_range.drop(all_star_date)
    if verbose:
        date_range = tqdm(date_range)
    for date in date_range:
        while True:
            try:
                lineups_list.append(lineups(str(date.date())))
                break
            except Exception as e:
                print(f"Error occurred: {e}. Retrying in 5 minutes...")
                time.sleep(300)
    lineups_list = [x for x in lineups_list if x is not None and not x.empty]
    return pd.concat(lineups_list, axis=0).reset_index(drop=True)

def lineup_to_predictors(
    lineup: dict,
    batting_data: pd.DataFrame,
    pitching_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Convert lineup data to a DataFrame of predictors for modeling.
    """
    away_lineup = [x["mlb_id"] for x in lineup["away"]["lineup"]]
    home_lineup = [x["mlb_id"] for x in lineup["home"]["lineup"]]
    date = pd.to_datetime(lineup["date"])

    away_batting = (
        batting_stats(away_lineup, batting_data)
        .assign(batting_order=range(1, 10))
        .melt(id_vars=["batting_order"], var_name="stat", value_name="value")
        .pivot_table(columns=["batting_order", "stat"], values="value")
        .reset_index(drop=True)
    )
    away_batting.columns = [f"{stat}_{order}" for order, stat in away_batting.columns]

    home_batting = (
        batting_stats(home_lineup, batting_data)
        .assign(batting_order=range(1, 10))
        .melt(id_vars=["batting_order"], var_name="stat", value_name="value")
        .pivot_table(columns=["batting_order", "stat"], values="value")
        .reset_index(drop=True)
    )
    home_batting.columns = [f"{stat}_{order}" for order, stat in home_batting.columns]

    batters = pd.concat([away_batting, home_batting], axis=0).reset_index(drop=True)
    pitchers = pitching_stats([lineup[x]["pitcher"]["mlb_id"] for x in ["home", "away"]], pitching_data)
    pitchers.columns = [f"{col}_opp_p" for col in pitchers.columns]

    matchup = pd.concat([batters, pitchers], axis=1)
    team_col = pd.DataFrame({
        "team": [lineup[x]["team"] for x in ["away", "home"]],
        "home_team": [lineup["home"]["team"]]*2,
        "date": [date]*2
    })
    matchup = pd.concat([team_col, matchup], axis=1)

    return matchup

def merge_outcome_to_lineups(
    lineups: pd.DataFrame,
    game_logs: pd.DataFrame,
    outcome: str = "R"
):
    """
    Merge game outcomes to lineup data for a specific statistic.
    
    The outcome should be one of: 'r', 'h', '2b', '3b', 'hr', 'rbi', 'bb', 'ibb', 
    'so', 'sf', 'roe', 'gdp', 'sb', 'cs', 'lob'
    """
    select_cols = ["team", "clean_date", "game_date_id", "home", outcome.lower(), "thr"]
    game_outcomes = game_logs[select_cols]
    game_outcomes.loc[:, "game_date_id"] = game_outcomes["game_date_id"].astype(int)
    lineups["team"] = lineups["team"].apply(lambda x: x.lower())
    lineups["game_date_id"] = lineups["game_date_id"].astype(int)
    nrow_prev = len(lineups)
    lineups = (
        pd
        .merge(
            lineups,
            game_outcomes,
            left_on=["team", "date", "game_date_id"],
            right_on=["team", "clean_date", "game_date_id"],
            how="left"
        )
        .groupby(["team", "date"])
        .filter(lambda x: x[outcome.lower()].notna().all())
        .drop(columns=["clean_date", "game_date_id"])
    )
    print(f"Dropped {nrow_prev - len(lineups)} rows with missing `{outcome.title()}` values")
    return lineups

def pin_dataframe(data: pd.DataFrame, name: str, directory: str):
    board = pins.board_folder(directory)
    _ = board.pin_write(data, name=name, type="csv")

def pitching_stats(ids: list, stats: pd.DataFrame) -> pd.DataFrame:
    """
    Extract and normalize pitching stats for given player IDs.
    """
    keep_cols = [
        "H", "R", "ER", "BB", "SO", "HR", "HBP", "ERA",
        "2B", "3B", "IBB", "GDP", "SF", "SB", "CS", "PO", "BF", "Pit",
        "Str", "StL", "StS", "GB/FB", "LD", "PU", "WHIP", "BAbip", "SO9",
        "SO/W"
    ]
    normalize_cols = [
        "H", "R", "ER", "BB", "SO", "HR", "HBP",
        "2B", "3B", "IBB", "GDP", "SF", "SB", "CS", "PO", "BF", "Pit"
    ]
    if stats.empty:
        return pd.DataFrame(columns=keep_cols)
    id_df = pd.DataFrame({"mlb_id": ids})
    stats["mlbID"] = stats["mlbID"].astype(int)
    stats = pd.merge(
        id_df, stats, left_on="mlb_id", right_on="mlbID", how="left"
    )
    for col in normalize_cols:
        stats[col] = stats[col] / stats["IP"]
    return stats[keep_cols]

def proxy(x: dict):
    proxy_url = f"http://{x['username']}:{x['password']}@{x['proxy_address']}:{x['port']}"
    return proxy_url

if __name__ == "__main__":
    output_dir = Path(__file__).resolve().parent.parent / "data"
    year = today().split("-")[0]
    print("Grabbing and pinning daily lineups ...")
    daily_lineups = lineups_up_to_date(date=today(), verbose=True)
    pin_dataframe(daily_lineups, f"lineups_{year}", output_dir)
    print("Grabbing and pinning game logs ...")
    logs = game_logs(date=today())
    pin_dataframe(logs, f"game_logs_{year}", output_dir)
    print("Merging and pinning outcomes to lineups ...")
    for outcome in ["R"]:
        outcomes_and_lineups = merge_outcome_to_lineups(daily_lineups, logs, outcome=outcome)
        pin_dataframe(
            outcomes_and_lineups,
            f"lineups_outcome_{outcome.lower()}_{year}",
            output_dir
        )
