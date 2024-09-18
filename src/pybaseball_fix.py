"""
The code in this file is taken from a pull request that has yet to be merged into
pybaseball. As a result, I've grabbed the code myself until pybaseball includes
this fix. For details see https://github.com/jldbc/pybaseball/pull/453
"""

from bs4 import BeautifulSoup
from datetime import date
import numpy as np
import pandas as pd
from pybaseball.cache import cache
from pybaseball.datasources.bref import BRefSession
from pybaseball.utils import sanitize_date_range
from typing import Optional, Union

session = BRefSession()

@cache.df_cache()
def batting_stats_range(start_dt: Optional[str] = None, end_dt: Optional[str] = None) -> pd.DataFrame:
    """
    Get all batting stats for a set time range. This can be the past week, the
    month of August, anything. Just supply the start and end date in YYYY-MM-DD
    format.
    """
    # make sure date inputs are valid
    start_dt_date, end_dt_date = sanitize_date_range(start_dt, end_dt)
    if start_dt_date.year < 2008:
        raise ValueError("Year must be 2008 or later")
    if end_dt_date.year < 2008:
        raise ValueError("Year must be 2008 or later")
    # retrieve html from baseball reference
    soup = get_soup(start_dt_date, end_dt_date, type="b")
    table = get_table(soup)
    table = table.dropna(how="all")  # drop if all columns are NA
    # scraped data is initially in string format.
    # convert the necessary columns to numeric.
    for column in ["Age", "#days", "G", "PA", "AB", "R", "H", "2B", "3B",
                    "HR", "RBI", "BB", "IBB", "SO", "HBP", "SH", "SF", "GDP",
                    "SB", "CS", "BA", "OBP", "SLG", "OPS", "mlbID"]:
        #table[column] = table[column].astype('float')
        table[column] = pd.to_numeric(table[column])
        #table['column'] = table['column'].convert_objects(convert_numeric=True)
    table = table.drop("", axis=1)
    return table

def get_soup(
    start_dt: Optional[Union[date, str]],
    end_dt: Optional[Union[date, str]],
    type: str
) -> BeautifulSoup:
    # get most recent standings if date not specified
    if((start_dt is None) or (end_dt is None)):
        print("Error: a date range needs to be specified")
        return None
    url = (
        "https://www.baseball-reference.com/leagues/daily.cgi?user_team=&bust_cache=&type={}&lastndays=7&dates=fromandto&fromandto={}.{}&level=mlb&franch=&stat=&stat_value=0"
        .format(type, start_dt, end_dt)
    )
    s = session.get(url).content
    # a workaround to avoid beautiful soup applying the wrong encoding
    s = s.decode("utf-8")
    return BeautifulSoup(s, features="lxml")

def get_table(soup: BeautifulSoup) -> pd.DataFrame:
    table = soup.find_all("table")[0]
    data = []
    headings = [th.get_text() for th in table.find("tr").find_all("th")][1:]
    headings.append("mlbID")
    data.append(headings)
    table_body = table.find("tbody")
    rows = table_body.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        row_anchor = row.find("a")
        mlbid = row_anchor["href"].split("mlb_ID=")[-1] if row_anchor else pd.NA  # ID str or nan
        cols = [ele.text.strip() for ele in cols]
        cols.append(mlbid)
        data.append([ele for ele in cols])
    df = pd.DataFrame(data)
    df = df.rename(columns=df.iloc[0])
    df = df.reindex(df.index.drop(0))
    return df

@cache.df_cache()
def pitching_stats_range(start_dt: Optional[str]=None, end_dt: Optional[str]=None) -> pd.DataFrame:
    """
    Get all pitching stats for a set time range. This can be the past week, the
    month of August, anything. Just supply the start and end date in YYYY-MM-DD
    format.
    """
    # ensure valid date strings, perform necessary processing for query
    start_dt_date, end_dt_date = sanitize_date_range(start_dt, end_dt)
    if start_dt_date.year < 2008:
        raise ValueError("Year must be 2008 or later")
    if end_dt_date.year < 2008:
        raise ValueError("Year must be 2008 or later")
    # retrieve html from baseball reference
    soup = get_soup(start_dt_date, end_dt_date, type="p")
    table = get_table(soup)
    table = table.dropna(how="all") # drop if all columns are NA
    #fix some strange formatting for percentage columns
    table = table.replace("---%", np.nan)
    #make sure these are all numeric
    for column in ["Age", "#days", "G", "GS", "W", "L", "SV", "IP", "H",
                    "R", "ER", "BB", "SO", "HR", "HBP", "ERA", "AB", "2B",
                    "3B", "IBB", "GDP", "SF", "SB", "CS", "PO", "BF", "Pit",
                    "WHIP", "BAbip", "SO9", "SO/W"]:
        table[column] = pd.to_numeric(table[column])
    #convert str(xx%) values to float(0.XX) decimal values
    for column in ["Str", "StL", "StS", "GB/FB", "LD", "PU"]:
        table[column] = table[column].replace("%","",regex=True).astype("float")/100

    table = table.drop("", axis=1)
    return table