import time
import itertools
import re
import bisect
import math
import pickle
import os
from datetime import datetime
import datetime as dt
import requests
import pandas as pd
from bs4 import BeautifulSoup
import pytz
from dotenv import load_dotenv
load_dotenv(dotenv_path='./config/s3_connection.env')
import fastparquet as fp
from requests_html import AsyncHTMLSession
import numpy as np
import s3fs
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import chromedriver_autoinstaller
from selenium.webdriver.support.ui import Select
import pickle
from selenium import webdriver
# pd.options.display.max_columns = 24


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

def convert_word_to_num(num_str):
    num = 0
    if '半' in num_str:
        num += 0.5

    if '球半' == num_str:
        num = 1.5
    elif '一' in num_str:
        num += 1
    elif '二' in num_str or '兩' in num_str or '两' in num_str:
        num += 2
    elif '三' in num_str:
        num += 3
    elif '四' in num_str:
        num += 4
    elif '五' in num_str:
        num += 5
    elif '六' in num_str:
        num += 6
    elif '七' in num_str:
        num += 7
    elif '八' in num_str:
        num += 8
    elif '九' in num_str:
        num += 9
    elif '十' in num_str:
        num += 10
    return num


def convert_handicap_string_to_float(handicap_str):
    if not handicap_str:
        return 0
    
    sign = 0
    handicap_str = handicap_str.replace('受让', '*')
    if '*' == handicap_str[0]:
        handicap_str = handicap_str[1:]
        sign = 1
    else:
        sign = -1

    divider = 2 if '/' in handicap_str else 1
    handicap_sum = sum([convert_word_to_num(x) for x in handicap_str.split('/')])
    handicap_sum*sign/divider
    return handicap_sum*sign/divider


def requests_fetch(url, headers, payload={}, action="GET"):
    response = None
    while True:
        try:
            response = requests.request(
                action, url, headers=headers, data=payload, timeout=5
            )
            if '404 - File or directory not found.' in response.text:
                return None
            
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
        print("refetch...", url)
        time.sleep(5)
    return response


async def asyncHTMLSessionFetch(url, headers, betting_companies=[]):
    soup_collection = {}
    soup = None
    while True:
        try:
            asession = AsyncHTMLSession()
            script = """
            () => {
                $(document).ready(function() {
                    $("#hSelect_hn").val(option_value).change();
                    $("#hSelect_an").val(option_value).change();
                })
            }
            """
            needRefresh = False
            for betting_company in betting_companies:
                option_value = company_to_info[betting_company]['company_id']
                new_script = script.replace('option_value', option_value)
                r = await asession.get(url, headers=headers)
                await r.html.arender(script=new_script)
                response = r.html.raw_html
                soup_collection[betting_company] = BeautifulSoup(response, "html.parser")
                soup = str(soup_collection[betting_company])
                if str(soup) is None or "操作太频繁了，请先歇一歇。" in str(soup):
                    needRefresh = True
        
            if not needRefresh and  r.status_code == 200:
                break
        except Exception as e:
            print(e)
        print("asyncHTMLSessionFetch refetch...", soup)
        time.sleep(5)
    return soup_collection


def find_index(lst, val):
    index = bisect.bisect_left(lst, val)
    return index


def format_recent_df(selected_recent_data_df, home_team):
    selected_recent_data_df['主紅牌'] = selected_recent_data_df['主場'].str.extract(r'(\d+)', expand=False)
    selected_recent_data_df['主紅牌'] = selected_recent_data_df['主紅牌'].fillna(0)
    selected_recent_data_df['主場'] = selected_recent_data_df['主場'].str.replace(r'\d+', '', regex=True).str.strip()
    selected_recent_data_df['客紅牌'] = selected_recent_data_df['客場'].str.extract(r'(\d+)', expand=False)
    selected_recent_data_df['客紅牌'] = selected_recent_data_df['客紅牌'].fillna(0)
    selected_recent_data_df['客場'] = selected_recent_data_df['客場'].str.replace(r'\d+', '', regex=True).str.strip()
    selected_recent_data_df['場地'] = '客'
    selected_recent_data_df.loc[
        selected_recent_data_df['主場'].str.contains(home_team)
    , '場地'] = '主'
    selected_recent_data_df.loc[
        selected_recent_data_df['主場'].str.contains('(中)')
    , '場地'] = '中'
    selected_recent_data_df['主場'] = selected_recent_data_df['主場'].str.replace('(中)', '')
    return selected_recent_data_df


def get_recent_data(match_id):
    url = f"https://zq.titan007.com/analysis/{match_id}.htm"
    options = Options()
    options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
    options.add_argument("--headless") #無頭模式
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), options=options)
    driver.get(url)
    while True:
        try:
            temp_info['page_source'] = driver.page_source
            if (
                not (
                    "操作太频繁了，请先歇一歇。" in temp_info['page_source']
                    or "404 Not Found !" in temp_info['page_source']
                )
            ):
                break
            print("refetch driver...", url)
            time.sleep(5)
        except  Exception as e:
            print('get_recent_data error...', e)
            print(driver.page_source)
        driver.refresh()

    home_team = driver.find_element(By.CLASS_NAME, "home").text.strip()
    is_home_field = False if '(中)' in home_team else True
    home_team = re.sub(r'\([^)]+\)', '', home_team)
    away_team = driver.find_element(By.CLASS_NAME, "guest").text.strip()
    recent_table = driver.find_element(By.ID, "porlet_10")
    league_name = driver.find_element(By.CLASS_NAME, "LName").text.strip()
    selected_company_list = ["澳*", '36*', 'Crow*']

    # 聯賽積分排名
    ranking_df = {}
    try:
        ranking_tables = driver.find_elements(By.XPATH, '//*[@id="porlet_5"]/div/table/tbody/tr[1]//tbody')
        if len(ranking_tables) >= 2:
            ranking_df['home'] = ranking_tables[0]
            ranking_df['away'] = ranking_tables[1]
            ranking_tables[0].get_attribute('innerHTML')
            for team in ranking_df:
                team_data = ranking_df[team].find_elements(By.TAG_NAME, 'tr')[1:]
                team_data = [[td.text for td in data.find_elements(By.TAG_NAME, 'td')] for data in team_data]
                ranking_df[team] = pd.DataFrame(team_data[1:], columns=team_data[0])
    except:
        try:
            ranking_tables = driver.find_elements(By.XPATH, '//*[@id="porlet_5"]/div/table/tbody/tr[1]//tbody//tbody')
            if len(ranking_tables) >= 2:
                ranking_df['home'] = ranking_tables[0]
                ranking_df['away'] = ranking_tables[1]
                ranking_tables[0].get_attribute('innerHTML')
                for team in ranking_df:
                    team_data = ranking_df[team].find_elements(By.TAG_NAME, 'tr')[1:]
                    team_data = [[td.text for td in data.find_elements(By.TAG_NAME, 'td')] for data in team_data]
                    ranking_df[team] = pd.DataFrame(team_data[1:], columns=team_data[0])
        except:
            print(f"Match {match_id}: No League Ranking")

    # 對賽往績
    all_vs_data_df = {}
    try:
    # all_vs_summary_df = {}
    # vs_summary_header = [
    #     'match_nums',
    #     'win_matches',
    #     'draw_matches',
    #     'lose_matches',
    #     'win_percentage',
    #     'win_handicap_percentage',
    #     'high_ball_percentage',
    #     'odd_percentage'
    # ]
        vs_odds_provider = Select(driver.find_element(By.XPATH, '//*[@id="hSelect_v"]'))
        for company in selected_company_list:
            vs_data_df = None
            vs_odds_provider.select_by_value(company_to_info[company]['company_id'])
            vs_table = driver.find_element(By.ID, "porlet_8")
            vs_data = [[data.text for data in vs_data.find_elements(By.TAG_NAME, "td")] for vs_data in vs_table.find_elements(By.TAG_NAME, "tr")][2:]
            if len(vs_data) > 2:
                vs_data = [vs_data[0][:6] + vs_data[1]] + vs_data[2:]
                vs_data_df = pd.DataFrame(vs_data[1:-1], columns=vs_data[0])
                vs_data_df = format_recent_df(vs_data_df, home_team)

                # vs_summary = [x.strip() for x in vs_data[-1][0].split(',')]
                # vs_summary = vs_summary[:-1] + vs_summary[-1].split(' ')
                # vs_summary = None if len(vs_summary) < 8 else [re.findall(r"[-+]?(?:\d*\.*\d+)", x.strip())[0] for x in vs_summary]
                # if vs_summary:
                #     vs_summary = {vs_summary_header[i]: vs_summary[i]  for i in range(len(vs_summary_header))}
            all_vs_data_df[company] = vs_data_df
    except:
        print(f"Match {match_id}: No VS matches")

    # 近期戰績
    all_recent_matches_df_dict = {}
    try:
        odds_providers = {
            "home": {'selection_box': Select(driver.find_element(By.XPATH, '//*[@id="hSelect_hn"]')), 'table_id': 'table_hn'},
            "away": {'selection_box': Select(driver.find_element(By.XPATH, '//*[@id="hSelect_an"]')), 'table_id': 'table_an'},
        }
        for team in odds_providers:
            recent_matches_df_dict = {}
            for company in selected_company_list:
                odds_providers['home']['selection_box'].select_by_value(company_to_info[company]['company_id'])
                recent_table = driver.find_element(By.XPATH, f'//*[@id="{odds_providers[team]["table_id"]}"]')
                selected_recent_data = [[data.text for data in recent_data.find_elements(By.TAG_NAME, "td")] for recent_data in recent_table.find_elements(By.TAG_NAME, "tr")]
                selected_recent_data_df = None
                if len(selected_recent_data) > 2:
                    selected_recent_data = [selected_recent_data[0][:6] + selected_recent_data[1]] + selected_recent_data[2:]
                    selected_recent_data_df = pd.DataFrame(selected_recent_data[1:-1], columns=selected_recent_data[0])
                    recent_matches_df_dict[company] = format_recent_df(selected_recent_data_df, home_team)
            all_recent_matches_df_dict[team] = recent_matches_df_dict
    except:
        print(f"Match {match_id}: No Recent Matches")

    driver.close()
    
    return {
        'home_team': home_team,
        'away_team': away_team,
        'is_home_field': is_home_field,
        'league': league_name,
        'recent_data': {
            'ranking_df': ranking_df,
            'all_vs_data_df': all_vs_data_df,
            'all_recent_matches_df_dict': all_recent_matches_df_dict
        }
    }


def get_had_df(match_id):
    url = f"https://1x2d.titan007.com/{match_id}.js?r=007133413129185127716"
    headers = {
    'authority': '1x2d.titan007.com',
    'accept': '*/*',
    'accept-language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'cookie': 'detailCookie=null; Registered=1',
    'referer': f'https://1x2.titan007.com/oddslist/{match_id}_2.htm',
    'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'sec-fetch-dest': 'script',
    'sec-fetch-mode': 'no-cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Mobile Safari/537.36'
    }
    
    html_page = None
    while True:
        try:
            response = requests_fetch(url, headers=headers)
            if response is None:
                return None
            if not response or "var matchname" not in response.text:
                continue
            else:
                html_page = response.text
                break
        except:
            global temp_info
            temp_info['html_page'] = html_page
            print("Error in Getting HAD:", match_id)
            time.sleep(5)
    
    regex_pattern = r'var game=Array\(\"(.*?)\"\);'
    search_result = re.search(regex_pattern, html_page)
    if not search_result:
        return None
    game = search_result.group(1).split('","')
    columns = [
        "cid", "id", "full_name", 
        "init_home_had", "init_draw_had", "init_away_had", "init_home_win_rate", "init_draw_win_rate", "init_away_win_rate", "init_returning_rate", 
        "last_home_had", "last_draw_had", "last_away_had", "last_home_win_rate", "last_draw_win_rate", "last_away_win_rate", "last_returning_rate",
        "home_kelly", "draw_kelly", "away_kelly",
        "last_update_datetime", "masked_company_name", "unknown1", "unknown2"
    ]
    had_df = pd.DataFrame([x.split('|') for x in game], columns=columns)
    had_df['masked_company_name'] = had_df['masked_company_name'].str.replace(r'\([^)]+\)', '', regex=True)
    had_df = had_df.drop_duplicates(subset=['masked_company_name'], keep='first')

    cid_to_company = had_df[['masked_company_name', 'cid']].set_index('cid').to_dict('index')
    cid_to_company = {k:v['masked_company_name'] for k, v in cid_to_company.items() if v['masked_company_name'] in company_to_info}
    had_df['company_id'] = had_df['masked_company_name'].map(lambda x: company_to_info[x]['company_id'] if x in company_to_info else np.nan)
    had_df = had_df.dropna(subset=['company_id'])
    return had_df


def get_handicap_odds(match_id, match_info=[]):
    url = f"https://vip.titan007.com/AsianOdds_n.aspx?id={match_id}&l=1"
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

    response = requests_fetch(url, headers=headers)
    print(
        f"API: Company List Response Status: {response.status_code} ; {match_id} {match_info}"
    )
    soup = BeautifulSoup(response.text, "html.parser")
    home_team = [x.text for x in soup.select('.home') if len(x)][0]
    is_home_field = False if '(中)' in home_team else True
    home_team = re.sub(r'\([^)]+\)', '', home_team).strip()
    away_team = [x.text for x in soup.select('.guest') if len(x)][0].strip()

    match_time = datetime.strptime(soup.find("span", {"class": "time"}).text.split('\xa0')[0], "%Y-%m-%d %H:%M")
    handicap_table = soup.find("table", {"id": "odds"})
    handicap_table_column = ['company', "multi_handicap_line", 'init_home_odds', 'init_handicap_line', 'init_away_odds', 'last_home_odds', 'last_handicap_line', 'last_away_odds', 'details']
    handicap_table_data = handicap_table.select('tr:not([style*="display: none"])')[2:]
    handicap_df = None
    if len(handicap_table_data) > 2:
        handicap_table_data = [[data.text for data in row.select('td:not([style*="display: none"])')] for row in handicap_table_data]
        handicap_df = pd.DataFrame(handicap_table_data, columns=handicap_table_column)
        handicap_df = handicap_df.drop(['multi_handicap_line', "details"],  axis=1)
        handicap_df = handicap_df[:-2]
        handicap_df = handicap_df.assign(numeric_init_handicap_line=handicap_df['init_handicap_line'].apply(convert_handicap_string_to_float))
        handicap_df = handicap_df.assign(numeric_last_handicap_line=handicap_df['last_handicap_line'].apply(convert_handicap_string_to_float))

    global selected_handicap_company_list, selected_handicap_company_id_list, company_id_to_name
    changed_odds_url_dict = {
        "澳*": [],
        '36*': [],
        'Crow*': [],
        '香港马*': [],
    }

    all_handicap_urls = [x['href'] for x in soup.select('a:contains("详")')]
    for handicap_url in all_handicap_urls:
        try:
            match = re.findall(r'(?<=companyID=)\d+', handicap_url)
            company_id = match[0]
            if company_id not in selected_handicap_company_id_list:
                continue
            changed_odds_url_dict[company_id_to_name[company_id]].append(handicap_url)
        except:
            print('Cannot find companyID:', handicap_url, match)

    changed_odds_dict = {}
    for company in changed_odds_url_dict:
        temp_odds = []
        for handicap_url in changed_odds_url_dict[company]:
            changed_url = "https://vip.titan007.com" + handicap_url
            temp_odds.append(get_odds_list(changed_url, home_team, away_team, match_time))

        flatten_odds = list(itertools.chain(*temp_odds))
        flatten_odds = [row for row in flatten_odds]
        flatten_odds_df = pd.DataFrame(flatten_odds)
        if len(flatten_odds_df) == 0:
            continue
        handicap_line_dict = {}
        for handicap_line, odds in flatten_odds_df.groupby(1):
            odds_table =  odds.sort_values(3).drop_duplicates()
            odds_table.columns = ['home_odds', 'handicap_line', 'away_odds', 'time', 'status', 'numeric_handicap_line']
            
            handicap_line_dict[handicap_line] = odds_table.loc[odds_table['status'] != '滾']
        changed_odds_dict[company] = handicap_line_dict
    return {'match_time': match_time, 'handicap_df': handicap_df, 'changed_odds_dict': changed_odds_dict}

    # html_page = response.text
    # global company_list
    # pattern = r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}"
    # matchTime = re.search(pattern, html_page).group()
    # matchTime = datetime.strptime(matchTime, "%Y-%m-%d %H:%M")

    # pattern = '<a href="(.*)" title="指数走势" target="_blank">详<\/a>'
    # bettingCompanies = re.findall(pattern, html_page)
    # for index in range(len(bettingCompanies)):
    #     bettingCompanies[index] = (
    #         "https://vip.titan007.com" + bettingCompanies[index]
    #     )
    #     for company in company_list:
    #         if (
    #             f"companyID={company_list[company]['company_id']}&"
    #             in bettingCompanies[index]
    #         ):
    #             if "url" not in company_list[company]:
    #                 company_list[company]["url"] = []
    #             company_list[company]["url"].append(bettingCompanies[index])

    # return matchTime, company_list


def get_odds_list(odds_url, home_team, away_team, match_time):
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
    response = requests_fetch(odds_url, headers=headers)
    decoded_content = response.content.decode("GB18030")
    soup = BeautifulSoup(decoded_content, "html.parser")
    selected_trs = [[td.text for td in tr.find_all("td")] for tr in soup.find("span", {"id": "odds2"}).find_all("tr")]
    selected_trs = [tr for tr in selected_trs if tr[-1] != '滚']
    current_month = match_time.month
    current_year =  match_time.year
    match_time_index = selected_trs[0].index('变化时间')
    for i in range(1, len(selected_trs)):
        odds_month = int(selected_trs[i][match_time_index].split("-")[0])
        year = (
            int(current_year) - 1
            if ((current_month == 1) and (odds_month == 12))
            else current_year
        )
        selected_trs[i][match_time_index] = datetime.strptime(
            f"{year}-{selected_trs[i][match_time_index]}", "%Y-%m-%d %H:%M"
        )

    odds_table = pd.DataFrame(selected_trs[1:], columns=selected_trs[0])
    odds_table = odds_table.assign(numeric_handicap_line=odds_table['盘口'].apply(convert_handicap_string_to_float))
    odds_table.rename({home_team: '主', away_team: '客'}, axis=1, inplace=True)
    odds_table = odds_table[["主", "盘口", "客", "变化时间", "状态", "numeric_handicap_line"]]
    return odds_table.values.tolist()


def get_latest_odds(date):
    global temp_info
    url = f"https://bf.titan007.com/football/big/Over_{date}.htm"
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
    print("starting...", url)
    response = requests_fetch(url, headers=headers)
    soup = BeautifulSoup(response.content.decode("GB18030"), "html.parser")
    daily_match_data = [[x.text for x in row.find_all('td')] + [row['sid']] for row in soup.find("table", {"id": "table_live"}).select('tr[sid]')]
    target_league = pd.read_csv("config/league.csv", header=None, encoding="utf8")
    target_league = target_league[target_league[0].notna()][0].unique().tolist()

    matches = []
    for match in daily_match_data:
        for league in target_league:
            if league in match[0]:
                matches.append(match)
                break

    matches_info = {}
    temp_info['matches'] = matches
    for match in matches:
        match_id = match[-1]
        handicap_info = get_handicap_odds(
            match_id, f"{match[0]} {match[3]} {match[5]}"
        )
        matches_info[match_id] = {
            "matchTime": handicap_info['match_time'],
            "handicap_df": handicap_info['handicap_df'],
            "changed_handicap_odds_dict": handicap_info['changed_odds_dict'],
        }
        matches_info[match[-1]]['had'] = get_had_df(match_id)
        if matches_info[match_id]['had'] is None:
            del matches_info[match_id]
            continue
        recent_data = get_recent_data(match_id)
        matches_info[match_id]['recent_data'] = recent_data['recent_data'] 
        matches_info[match_id]['is_home_field'] = recent_data['is_home_field']
        matches_info[match_id]['league'] = recent_data['league']
        matches_info[match_id]['home_team'] = recent_data['home_team']
        matches_info[match_id]['away_team'] = recent_data['away_team']

    temp_info['matches_info'] = matches_info
    return matches_info

chromedriver_autoinstaller.install()
company_to_info = {
    '香港马*': {'company_id': '48', 'url': [], 'odds': [], 'cid': '432'},
    '36*': {'company_id': '8', 'url': [], 'odds': [], 'cid': '281'},
    'Crow*': {'company_id': '3', 'url': [], 'odds': [], 'cid': '545'},
    '澳*': {'company_id': '1', 'url': [], 'odds': [], 'cid': '80'},
    '易*': {'company_id': '12', 'url': [], 'odds': [], 'cid': '90'},
    '伟*': {'company_id': '14', 'url': [], 'odds': [], 'cid': '81'},
    '明*': {'company_id': '17', 'url': [], 'odds': [], 'cid': '517'},
    '10*': {'company_id': '22', 'url': [], 'odds': [], 'cid': '16'},
    '金宝*': {'company_id': '23', 'url': [], 'odds': [], 'cid': '499'},
    '12*': {'company_id': '24', 'url': [], 'odds': [], 'cid': '18'},
    '利*': {'company_id': '31', 'url': [], 'odds': [], 'cid': '474'},
    '盈*': {'company_id': '35', 'url': [], 'odds': [], 'cid': '659'},
    '18*': {'company_id': '42', 'url': [], 'odds': [], 'cid': '976'},
    'Interwet*': {'company_id': '19', 'url': [], 'odds': [], 'cid': '104'},
    # "平*": {"company_id": "47", "url": [], "odds": []},
}
cid_to_company = {
    '281': '36*',
    '81': '伟*',
    '90': '易*',
    '104': 'Interwet*',
    '16': '10*',
    '18': '12*',
    '976': '18*',
    '545': 'Crow*',
    '80': '澳*',
    '499': '金宝*',
    '474': '利*',
    '517': '明*',
    '432': '香港马*',
    '659': '盈*'
}
company_to_cid = {
    '36*': '281',
    '伟*': '81',
    '易*': '90',
    'Interwet*': '104',
    '10*': '16',
    '12*': '18',
    '18*': '976',
    'Crow*': '545',
    '澳*': '80',
    '金宝*': '499',
    '利*': '474',
    '明*': '517',
    '香港马*': '432',
    '盈*': '659'
}
selected_handicap_company_list = ["澳*", '36*', 'Crow*', '香港马*']
selected_handicap_company_id_list = ['1', '8', '3', '48']
company_id_to_name = {
    '1': "澳*",
    '8': '36*',
    '3': 'Crow*',
    '48': '香港马*',
}
temp_info = {}

s3_bucket = 'football-screener/data'
fs = connect_to_s3()

# collected_date = [date[:-4] for date in os.listdir('data')]
# cur_datetime = datetime.now()
delta = dt.timedelta(days=1)
# min_date = min(collected_date) if len(collected_date) > 0 else (cur_datetime - delta).strftime('%Y%m%d') if cur_datetime.hour < 12 else cur_datetime.strftime('%Y%m%d')
# target_date = datetime.strptime(min_date, '%Y%m%d') - delta
target_date = dt.date(2022, 2, 1)
while True:
    formatted_date = target_date.strftime('%Y%m%d')
    matches_info = get_latest_odds(formatted_date)
    # with open(os.path.join('data', f'{formatted_date}.pkl'), 'wb') as file:
    #     pickle.dump(matches_info, file)

    pickle.dump(matches_info, fs.open(f's3://{s3_bucket}/{formatted_date}.pkl', 'wb'))
    # with open('data/20230925.pkl', 'rb') as file:
    #     matches_info = pickle.load(file)
    target_date -= delta
