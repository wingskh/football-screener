import time
import itertools
import re
import bisect
import math
import pickle
import os
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
import pytz
from dotenv import load_dotenv
load_dotenv(dotenv_path='./config/s3_connection.env')
import fastparquet as fp
from handicap_translate import handicap_zh2str
from bs4 import BeautifulSoup
from requests_html import AsyncHTMLSession


def requestsFetch(url, headers, payload={}, action="GET"):
    response = None
    while True:
        try:
            response = requests.request(
                action, url, headers=headers, data=payload, timeout=5
            )
            if (
                not (
                    response is None
                    or "操作太频繁了，请先歇一歇。" in response.text
                    or "404 Not Found !" in response.text
                )
                and response.status_code == 200
            ):
                break
        except Exception as e:
            print(e)
        print("refetch...", None if response is None else response.text)
        time.sleep(5)
    return response


def find_index(lst, val):
    index = bisect.bisect_left(lst, val)
    return index


def get_recent_matches(matchId):
    pass

matchId = '2408247'
url = f"https://zq.titan007.com/analysis/{matchId}.htm"

headers = {
  'authority': 'zq.titan007.com',
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
  'accept-language': 'zh-TW,zh;q=0.9',
  'cache-control': 'max-age=0',
  'cookie': 'Registered=1; Hm_lvt_6ae6f47a344eddfeef4d7bfc2cce7742=1695972380; fAnalyCookie=1^8^1^0^1^8^1^0^1^8^1^0^1^1^1^0; Hm_lpvt_6ae6f47a344eddfeef4d7bfc2cce7742=1695972400',
  'if-modified-since': 'Fri, 29 Sep 2023 02:04:56 GMT',
  'if-none-match': 'W/"3a40ba5779f2d91:0"',
  'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-dest': 'document',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-site': 'none',
  'sec-fetch-user': '?1',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
  'Accept': '*/*',
  'Accept-Language': 'zh-TW,zh;q=0.9',
  'Cache-Control': 'max-age=0',
  'Connection': 'keep-alive',
  'Referer': 'https://zq.titan007.com/analysis/2408247.htm',
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'no-cors',
  'Sec-Fetch-Site': 'cross-site',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
  'referer': 'https://zq.titan007.com/analysis/2408247.htm',
  'x-requested-with': 'XMLHttpRequest',
  'If-None-Match': '2b5ca7de392480259ca1ca9bebd19b68',
  'Origin': 'https://zq.titan007.com'
}

response = requestsFetch(url, headers=headers)
html_page = response.content.decode('utf-8').replace('\r\n', '\n')
asession = AsyncHTMLSession()
r = await asession.get(url, headers=headers)
await r.html.arender()
resp=r.html.raw_html
soup = BeautifulSoup(resp, "html.parser") #BeautifulSoup(response.content, "html.parser")

ranking_table = soup.find("div", {"id": "porlet_5"})
vs_table = soup.find("div", {"id": "porlet_8"})
recent_table = soup.find("div", {"id": "porlet_10"})

ranking_tables = ranking_table.find('table').find('tbody').find_all('table')
home_ranking_table = ranking_tables[0]
away_ranking_table = ranking_tables[1]

home_data = home_ranking_table.find_all('tr')[1:]
home_data = [[td.text for td in data.find_all('td')] for data in home_data]
home_ranking_df = pd.DataFrame(home_data[1:], columns=home_data[0])

away_data = away_ranking_table.find_all('tr')[1:]
away_data = [[td.text for td in data.find_all('td')] for data in away_data]
away_ranking_df = pd.DataFrame(away_data[1:], columns=away_data[0])

# vs_table = home_ranking_table = away_ranking_table = home_recent_table = away_recent_table = None
vs_table = soup.find("div", {"id": "porlet_8"})
vs_table.find_all("tr")

vs_data = [[data.text for data in vs_data] for vs_data in vs_table.find_all("tr")][2:]
vs_data = [vs_data[0][:6] + vs_data[1]] + vs_data[2:]
vs_data_df = pd.DataFrame(vs_data[1:-1], columns=vs_data[0])
vs_data_df

import re
[x.strip() for x in vs_data[-1][0].split(',')]

vs_summary = [x.strip() for x in vs_data[-1][0].split(',')]
vs_summary = vs_summary[:-1] + vs_summary[-1].split(' ')
vs_summary = [re.findall(r"[-+]?(?:\d*\.*\d+)", x.strip())[0] for x in vs_summary]

vs_summary_header = ['match_nums',
 'win_matches',
 'draw_matches',
 'lose_matches',
 'win_percentage',
 'win_handicap_percentage',
 'high_ball_percentage',
 'odd_percentage'
]
vs_summary = {vs_summary_header[i]: vs_summary[i]  for i in range(len(vs_summary_header))}
vs_summary



def get_handicap_odds(matchId, matchInfo):
    url = f"https://vip.titan007.com/AsianOdds_n.aspx?id={matchId}&l=1"

    headers = {
        "authority": "vip.titan007.com",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "zh-TW,zh-HK;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6",
        "cache-control": "max-age=0",
        "cookie": "Hm_lvt_a88664a99dbcb9c7c07dc420114041b3=1678761738; Hm_lpvt_a88664a99dbcb9c7c07dc420114041b3=1678761993",
        "referer": "https://live.titan007.com/indexall_big.aspx",
        "sec-ch-ua": '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-site",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    }

    response = requestsFetch(url, headers=headers)
    print(
        f"API: Company List Response Status: {response.status_code} ; {matchId} {matchInfo}"
    )
    html_page = response.text

    company_list = {
        "香港馬會": {"id": "48", "url": [], "odds": []},
        "365": {"id": "8", "url": [], "odds": []},
        "Crown": {"id": "3", "url": [], "odds": []},
        "澳*": {"id": "1", "url": [], "odds": []},
        "易*": {"id": "12", "url": [], "odds": []},
        "伟*": {"id": "14", "url": [], "odds": []},
        "明*": {"id": "17", "url": [], "odds": []},
        "10*": {"id": "22", "url": [], "odds": []},
        "金宝*": {"id": "23", "url": [], "odds": []},
        "12*": {"id": "24", "url": [], "odds": []},
        "利*": {"id": "31", "url": [], "odds": []},
        "盈*": {"id": "35", "url": [], "odds": []},
        "18*": {"id": "42", "url": [], "odds": []},
        "平*": {"id": "47", "url": [], "odds": []},
        "Interwet*": {"id": "19", "url": [], "odds": []},
    }

    pattern = r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}"
    global html_page_temp
    html_page_temp = html_page
    matchTime = re.search(pattern, html_page).group()
    matchTime = datetime.strptime(matchTime, "%Y-%m-%d %H:%M")

    if "香港马*" in html_page:
        pattern = '<a href="(.*)" title="指数走势" target="_blank">详<\/a>'
        bettingCompanies = re.findall(pattern, html_page)
        for index in range(len(bettingCompanies)):
            bettingCompanies[index] = (
                "https://vip.titan007.com" + bettingCompanies[index]
            )
            for company in company_list:
                if (
                    f"companyID={company_list[company]['id']}&"
                    in bettingCompanies[index]
                ):
                    if "url" not in company_list[company]:
                        company_list[company]["url"] = []
                    company_list[company]["url"].append(bettingCompanies[index])

    return matchTime, company_list


def get_odds_list(odds_url):
    headers = {
        "authority": "vip.titan007.com",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "zh-TW,zh-HK;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6",
        "cache-control": "max-age=0",
        "cookie": "Hm_lvt_a88664a99dbcb9c7c07dc420114041b3=1678761738; ShowCIDs=23; Hm_lpvt_a88664a99dbcb9c7c07dc420114041b3=1678780236",
        "sec-ch-ua": '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    }
    response = requestsFetch(odds_url, headers=headers)
    print(f"API: OddsList Response Status: {response.status_code} ; {odds_url}")
    decoded_content = response.content.decode("GB18030")
    tr_elements = []
    soup = BeautifulSoup(decoded_content, "html.parser")
    for tr in soup.find_all("tr")[1:]:
        tr_elements.append([td.text for td in tr.find_all("td")])
    odds_list = [tr_element[-5:] for tr_element in tr_elements][1:]
    odds_list
    odds_list = [oddsInfo for oddsInfo in odds_list if oddsInfo[-1] != "滚"]

    current_datetime = datetime.now(pytz.timezone("Asia/Hong_Kong"))
    current_month = current_datetime.month
    current_year = current_datetime.year

    for i in range(len(odds_list)):
        odds_month = int(odds_list[i][3].split("-")[0])
        year = (
            int(current_year) - 1
            if ((current_month == 1) and (odds_month == 12))
            else current_year
        )
        odds_list[i][3] = datetime.strptime(
            f"{year}-{odds_list[i][3]}", "%Y-%m-%d %H:%M"
        )

    return odds_list


def get_latest_odds():
    url = f"https://livestatic.titan007.com/vbsxml/bfdata_ut.js?r=007{int(datetime.now().timestamp())}000"
    url = "https://zq.titan007.com/analysis/2400444.htm"
    headers = {
        "authority": "livestatic.titan007.com",
        "accept": "*/*",
        "accept-language": "zh-TW,zh-HK;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6",
        "referer": "https://live.titan007.com/oldIndexall_big.aspx",
        "sec-ch-ua": '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "script",
        "sec-fetch-mode": "no-cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    }

    response = requestsFetch(url, headers=headers)
    dailyMatchData = response.text

    pattern = r'A\[(\d+)\]="(.*?)\"\.split\(\'\^\'\);'
    raw_matches = re.findall(pattern, dailyMatchData)
    raw_matches = [
        match[1]
        .replace("<font color=#880000>(中)</font>", "")
        .replace('A\[(\d+)\]="', "")
        .replace("\".split\('\^'\);", "")
        .split("^")
        for match in raw_matches
    ]
    target_league = pd.read_csv("config/league.csv", header=None, encoding="utf8")
    target_league = target_league[target_league[0].notna()][0].unique().tolist()

    matches = []
    for match in raw_matches:
        for league in target_league:
            if league in match[3]:
                matches.append(match)
                break
                
    matches_info = {}

    matches = [match for match in matches if match[0] == "2400444"]

    for match in matches:
        match_time, company_list = get_handicap_odds(
            match[0], f"{match[3]} {match[6]} {match[9]}"
        )
        matches_info[match[0]] = {
            "league": match[3],
            "homeTeam": match[6],
            "awayTeam": match[9],
            "matchTime": match_time,
            "handicapOdds": {},
        }

        for company in company_list:
            matches_info[match[0]]["handicapOdds"][company] = {}
            for odds_url in company_list[company]["url"]:
                company_list[company]["odds"].append(get_odds_list(odds_url))

            flatten_odds = list(itertools.chain(*company_list[company]["odds"]))
            flatten_odds_df = pd.DataFrame(flatten_odds)
            if len(flatten_odds_df) == 0:
                continue
            for handicap_line, odds in flatten_odds_df.groupby(1):
                odds_table =  odds.sort_values(3).drop_duplicates()
                odds_table.columns = ['home_odds', 'handicap_line', 'away_odds', 'time', 'status']
                matches_info[match[0]]["handicapOdds"][company][
                    handicap_line
                ] = odds_table.loc[odds_table['status'] != '滾']


    print()
    global temp_matches, temp_values
    temp_matches = matches_info
    print('---------------------------------------------')
    print(matches_info)


