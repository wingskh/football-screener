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
from flask_cors import CORS, cross_origin
from flask import Flask, make_response
import pytz
from flask_socketio import SocketIO
from dotenv import load_dotenv
load_dotenv(dotenv_path='./config/s3_connection.env')
import s3fs
import fastparquet as fp
import schedule
from handicap_translate import handicap_zh2str
from scrapy import start_scrapy

def connect_to_s3():
    fs = s3fs.S3FileSystem(
        anon=False,
        use_ssl=True,
        client_kwargs={
            "aws_access_key_id": os.environ['S3_ACCESS_KEY'],
            "aws_secret_access_key": os.environ['S3_SECRET_KEY'],
            "verify": True,
        }
    )
    return fs

def read_matches():
    global fs, s3_filepath
    with fs.open(s3_filepath) as f:
        df = fp.ParquetFile(f).to_pandas()
    return df

def write_matches(df, filename='matches.parquet'):
    global fs
    with fs.open(s3_bucket + filename, 'wb') as f:
        fp.write(f, df, compression='snappy')

s3_bucket = 'football-screener/'
s3_filepath = s3_bucket + 'matches.parquet'
fs = connect_to_s3()
is_getting_latest_odds = False
app = Flask(__name__, static_folder="frontend/build", static_url_path="/")
app.config["SECRET_KEY"] = "secret!"
socketio = SocketIO(app, cors_allowed_origins="*", engineio_logger=True, logger=True)

CORS(app)


def prepare_table():
    matches_df = read_matches()
    matches_df = matches_df.sort_values(["matchTime", "league"])
    matches_df["matchTime"] = matches_df["matchTime"].dt.strftime("%Y-%m-%d %H:%M")
    return matches_df[matches_df["homeTeam"].notna()]


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


def get_offered_company_list(matchId, matchInfo):
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
    global is_getting_latest_odds
    if is_getting_latest_odds:
        return
    
    is_getting_latest_odds = True
    ori_matches_df = read_matches()

    url = f"https://livestatic.titan007.com/vbsxml/bfdata_ut.js?r=007{int(datetime.now().timestamp())}000"
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
                
    latest_match_id = [match[0] for match in matches]
    ori_matches_df = ori_matches_df[ori_matches_df["matchID"].isin(latest_match_id)]
    ori_match_id = ori_matches_df["matchID"].unique()
    matches = [match for match in matches if match[0] not in ori_match_id]
    matches_info = {}

    for match in matches:
        match_time, company_list = get_offered_company_list(
            match[0], f"{match[3]} {match[6]} {match[9]}"
        )
        matches_info[match[0]] = {
            "league": match[3],
            "homeTeam": match[6],
            "awayTeam": match[9],
            "matchTime": match_time,
            "odds": {},
        }

        if len(company_list["香港馬會"]["url"]) == 0:
            print(f"{match[3]} {match[6]} {match[9]}: no HKJC")
            continue

        company_list["香港馬會"]["odds"].append(
            get_odds_list(company_list["香港馬會"]["url"][0])
        )

        for company in company_list:
            matches_info[match[0]]["odds"][company] = {}
            if company != ["香港馬會"]:
                for odds_url in company_list[company]["url"]:
                    company_list[company]["odds"].append(get_odds_list(odds_url))

            flatten_odds = list(itertools.chain(*company_list[company]["odds"]))
            flatten_odds_df = pd.DataFrame(flatten_odds)
            if len(flatten_odds_df) == 0:
                continue
            for handicap_line, odds in flatten_odds_df.groupby(1):
                matches_info[match[0]]["odds"][company][
                    handicap_line
                ] = odds.sort_values(3).drop_duplicates()

            matches_info[match[0]]["hkjc"] = {
                "hkjc_init_handicap_line": company_list["香港馬會"]["odds"][0][-1][1],
                "hkjc_init_odds_time": company_list["香港馬會"]["odds"][0][-1][3],
                "hkjc_init_home_odds": float(company_list["香港馬會"]["odds"][0][-1][0]),
                "hkjc_init_away_odds": float(company_list["香港馬會"]["odds"][0][-1][2]),
                "oriOdds": company_list,
            }
            matches_info[match[0]]["oriOdds"] = company_list

    matches_info_df = {}
    for match in matches_info:
        matches_info_df[match] = {}
        if "hkjc" not in matches_info[match]:
            continue
        company_diff_list = {}
        large_diff_companies = {}
        reversed_companies = {"home": [], "away": []}
        for company in matches_info[match]["odds"]:
            if (
                (company == "香港馬會")
                or (len(matches_info[match]["odds"][company]) == 0)
                or (
                    matches_info[match]["hkjc"]["hkjc_init_handicap_line"]
                    not in matches_info[match]["odds"][company]
                )
            ):
                continue

            company_match_time_list = matches_info[match]["odds"][company][
                matches_info[match]["hkjc"]["hkjc_init_handicap_line"]
            ][3].tolist()

            index = find_index(
                company_match_time_list,
                matches_info[match]["hkjc"]["hkjc_init_odds_time"],
            )
            index = index if index != len(company_match_time_list) else index - 1
            if (index != len(company_match_time_list) - 1) and (
                company_match_time_list[index]
                - matches_info[match]["hkjc"]["hkjc_init_odds_time"]
                > (
                    company_match_time_list[index + 1]
                    - matches_info[match]["hkjc"]["hkjc_init_odds_time"]
                )
                * 2
            ):
                index += 1

            other_init_record = matches_info[match]["odds"][company][
                matches_info[match]["hkjc"]["hkjc_init_handicap_line"]
            ].iloc[index]
            other_init_home_odds = float(other_init_record[0])
            other_init_away_odds = float(other_init_record[2])

            other_hkjc_diff = (
                other_init_home_odds
                - matches_info[match]["hkjc"]["hkjc_init_home_odds"]
            ) - (
                other_init_away_odds
                - matches_info[match]["hkjc"]["hkjc_init_away_odds"]
            )
            is_reversed = math.copysign(
                1,
                matches_info[match]["hkjc"]["hkjc_init_home_odds"]
                - matches_info[match]["hkjc"]["hkjc_init_away_odds"],
            ) != math.copysign(1, other_init_home_odds - other_init_away_odds)
            
            company_diff_list[company] = other_hkjc_diff
            if abs(other_hkjc_diff) >= 0.15:
                large_diff_companies[company] = other_hkjc_diff

            if is_reversed:
                if (
                    matches_info[match]["hkjc"]["hkjc_init_home_odds"]
                    < matches_info[match]["hkjc"]["hkjc_init_away_odds"]
                ):
                    reversed_companies["home"].append(company)
                else:
                    reversed_companies["away"].append(company)

        hkjc_init_handicap_line_int = matches_info[match]["hkjc"][
            "hkjc_init_handicap_line"
        ]
        if "受让" in hkjc_init_handicap_line_int:
            hkjc_init_handicap_line_int = hkjc_init_handicap_line_int.replace("受让", "-")
        else:
            hkjc_init_handicap_line_int = "+" + hkjc_init_handicap_line_int
        int_raw_handicap_line = handicap_zh2str[hkjc_init_handicap_line_int[1:]]
        hkjc_init_handicap_line_int = (
            hkjc_init_handicap_line_int[0] + int_raw_handicap_line
            if int_raw_handicap_line != "0"
            else int_raw_handicap_line
        )

        matches_info[match]["homeOdds"] = matches_info[match]["hkjc"][
            "hkjc_init_home_odds"
        ]
        matches_info[match]["awayOdds"] = matches_info[match]["hkjc"][
            "hkjc_init_away_odds"
        ]
        matches_info[match]["handicap"] = hkjc_init_handicap_line_int
        matches_info[match]["diff"] = list(company_diff_list.keys())
        matches_info[match]["diffCount"] = (
            f"Count: {len(list(large_diff_companies.keys()))} ; Sum: "
            + "{:.2f}".format(
                sum([large_diff_companies[company] for company in large_diff_companies])
            )
        )
        matches_info[match]["diffSum"] = "{:.2f}".format(
            sum(company_diff_list.values())
        )
        matches_info[match]["reversedDetail"] = reversed_companies
        home_reversed_companies_count = len(reversed_companies["home"])
        away_reversed_companies_count = len(reversed_companies["away"])
        matches_info[match][
            "reversedCount"
        ] = f"主: {home_reversed_companies_count} ; 客: {away_reversed_companies_count}"
        matches_info[match]["diffDetail"] = "\n".join(
            [
                f"{company}: " + "{:.2f}".format(diff)
                for company, diff in company_diff_list.items()
            ]
        )
        matches_info[match]["reversedHome"] = "\n".join(
            [company for company in reversed_companies["home"]]
        )
        matches_info[match]["reversedAway"] = "\n".join(
            [company for company in reversed_companies["away"]]
        )

        for key in [
            "matchTime",
            "league",
            "homeTeam",
            "awayTeam",
            "homeOdds",
            "handicap",
            "awayOdds",
            "diffSum",
            "diffDetail",
            "diffCount",
            "reversedCount",
            "reversedHome",
            "reversedAway",
        ]:
            matches_info_df[match][key] = matches_info[match][key]

    matches_info_df = pd.DataFrame.from_dict(matches_info_df, orient="index")
    matches_info_df = matches_info_df.reset_index().rename(columns={"index": "matchID"})

    combined_matches_info_df = pd.concat([ori_matches_df, matches_info_df])

    write_matches(combined_matches_info_df)

    print("Function: done get_latest_odds")
    is_getting_latest_odds = False
    return combined_matches_info_df

@socketio.on("get_odds")
def get_odds():
    print('SocketIO: received get_odds')
    matches_df = prepare_table()
    socketio.emit("receive_odds", matches_df.to_json(orient="records"))
    get_latest_odds()

@app.route("/", methods=["GET"])
@cross_origin()
def index():
    return app.send_static_file("index.html")


@app.errorhandler(404)
@cross_origin()
def not_found(e):
    return app.send_static_file("index.html")


if __name__ == "__main__":
    schedule.every().hour.do(get_odds)
    socketio.run(
        app, host="0.0.0.0", debug=True, port=int(os.environ.get("PORT", 5000))
    )
    start_scrapy()
    while True:
        schedule.run_pending()
        time.sleep(3600)
