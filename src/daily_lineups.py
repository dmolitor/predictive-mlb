from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
import json
import requests as req
from unidecode import unidecode

class Lineups:
    def __init__(self, date, html):
        self.date = date
        self.lineups = html.select("div.starting-lineups__matchup")

def extract_lineups(content: dict, date: str) -> dict:
    lineup_json = {"date": date, "away": {}, "home": {}}

    # Get team information
    for team in ["away", "home"]:
        lineup_json[team]["team"] = (
            content
            .select_one(
                (
                    "span.starting-lineups__team-name"
                    + f".starting-lineups__team-name--{team}"
                )
            )
            .text
            .strip()
        )
        lineup_json[team]["team_code"] = (
            content
            .select_one(
                (
                    "span.starting-lineups__team-name"
                    + f".starting-lineups__team-name--{team}"
                )
            )
            .select_one("a.starting-lineups__team-name--link")
            .get_attribute_list("data-tri-code")
            [0]
        )

    # Get starting pitcher information
    pitchers = (
        content
        .select("div.starting-lineups__pitcher-name")
    )
    assert len(pitchers) == 2, f"Two pitchers expected; found {len(pitchers)}"
    pitcher_mlb_ids = []
    pitcher_names = []
    pitcher_urls = []
    for x in pitchers:
        pitcher_name = x.select_one("a.starting-lineups__pitcher--link")
        pitcher_url = x.select_one("a.starting-lineups__pitcher--link")
        if pitcher_name is None:
            pitcher_mlb_ids.append(None)
            pitcher_names.append(None)
            pitcher_urls.append(None)
        else:
            url = pitcher_url.get_attribute_list("href")[0]
            mlb_id = int(last(url.split("-")))
            pitcher_mlb_ids.append(mlb_id)
            pitcher_names.append(unidecode(pitcher_name.text))
            pitcher_urls.append(url)
    lineup_json["away"]["pitcher"] = {
        "mlb_id": pitcher_mlb_ids[0],
        "name": pitcher_names[0],
        "url": pitcher_urls[0]
    }
    lineup_json["home"]["pitcher"] = {
        "mlb_id": pitcher_mlb_ids[1],
        "name": pitcher_names[1],
        "url": pitcher_urls[1]
    }

    # Get lineups
    for team in ["away", "home"]:
        lineup = (
            content
            .select_one(
                f"ol.starting-lineups__team.starting-lineups__team--{team}"
            )
        )
        players = []
        for player in lineup.select("a.starting-lineups__player--link"):
            url = player.get_attribute_list("href")[0]
            mlb_id = int(last(url.split("-")))
            players.append(
                {
                    "mlb_id": mlb_id,
                    "name": unidecode(player.text),
                    "url": url
                }
            )
        lineup_json[team]["lineup"] = players
    
    # Drop teams that are missing lineup or pitcher information
    valid_lineup = (
        lineup_json["home"]["lineup"] and lineup_json["away"]["lineup"] and 
        lineup_json["home"]["pitcher"]["name"] is not None and 
        lineup_json["away"]["pitcher"]["name"] is not None
    )
    if valid_lineup:
        return lineup_json
    else:
        return None

def last(x):
    return x[len(x)-1]

def lineups_by_date(date = datetime.now().strftime("%Y-%m-%d")):
    response = req.get(f"https://www.mlb.com/starting-lineups/{date}")
    response.raise_for_status()
    html = BeautifulSoup(response.content, "html.parser")
    return Lineups(date, html)

def previous_day(date_str):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    previous_date = date - timedelta(days=1)
    return previous_date.strftime("%Y-%m-%d")

def today():
    return datetime.now(tz=timezone(timedelta(hours=-5))).strftime("%Y-%m-%d")

if __name__ == "__main__":

    starting_lineups = []
    lineups = lineups_by_date(date=today())
    for matchup in lineups.lineups:
        starting_lineups.append(extract_lineups(matchup, lineups.date))
    print(json.dumps(starting_lineups, indent=4))