# import pyodbc
import pymssql
from riotwatcher import LolWatcher, ApiError
from credentials import passwords  # contains "passwords" dictionary
import time
import random
from datetime import datetime

# "Server: Region" format
#  Server used by Summoner-v4, League-v4
#  Region used by Match-v5
api_regions = {
    'BR1': 'AMERICAS',
    'EUN1': 'EUROPE',
    'EUW1': 'EUROPE',
    'JP1': 'ASIA',
    'KR': 'ASIA',
    'LA1': 'AMERICAS',
    'LA2': 'AMERICAS',
    'NA1': 'AMERICAS',
    'OC1': 'AMERICAS',
    'RU': 'EUROPE',
    'TR1': 'EUROPE'
}

# Create LolWatcher object to fetch data from API (get password form and outside file)
api_connector = LolWatcher(api_key=passwords["api_key"])


def get_summoner_v4_info(server: str, puuid: str):
    """
    Requires server (ex. EUN1) and puuid, Returns API response body if found.
    Returns False if not found, tries again if request failed.

    """
    while True:
        try:
            summoner_info = api_connector.summoner.by_puuid(server, puuid)
            result = {
                "summoner_id": summoner_info['id'],
                "account_id": summoner_info['accountId'],
                "puuid": summoner_info['puuid'],
                "summoner_name": summoner_info['name'],
                "profile_icon_id": summoner_info['profileIconId'],
                "revision_date": summoner_info['revisionDate'],
                "summoner_level": summoner_info['summonerLevel']
            }
            # print("Fetched summoner info with Summoner-v4")
            # print(result)
            return result
        except ApiError as err:
            if err.response.status_code == 404:
                print("Summoner-v4 404: no summoner with such puuid on that server")
                return False
            else:
                # Wait 5 seconds and try again
                print("Summoner-v4 retry in 5 seconds")
                time.sleep(5)


def get_league_v4_info(server: str, summoner_id: str):
    """
    Requires server (ex. EUN1) and summoner_id, returns response body. If not found tries again.
    """
    # todo return False if 404?
    try:
        result = api_connector.league.by_summoner(server, summoner_id)
        # print("Fetched league info with League-v4")
        # print(result)
        to_return = {
            'tier_solo_duo': 'unranked',
            'rank_solo_duo': 'unranked',
            'wins_solo_duo': 0,
            'loses_solo_duo': 0,
            'tier_flex': 'unranked',
            'rank_flex': 'unranked',
            'wins_flex':  0,
            'loses_flex': 0
        }
        if (len(result)) > 0:
            for ranked_que in result:
                if ranked_que['queueType'] == 'RANKED_SOLO_5x5':
                    to_return['tier_solo_duo'] = ranked_que['tier']
                    to_return['rank_solo_duo'] = ranked_que['rank']
                    to_return['wins_solo_duo'] = ranked_que['wins']
                    to_return['loses_solo_duo'] = ranked_que['losses']
                    # print("Solo duo:", tier_solo_duo, rank_solo_duo, wins_solo_duo, loses_solo_duo)
                elif ranked_que['queueType'] == 'RANKED_FLEX_SR':
                    to_return['tier_flex'] = ranked_que['tier']
                    to_return['rank_flex'] = ranked_que['rank']
                    to_return['wins_flex'] = ranked_que['wins']
                    to_return['loses_flex'] = ranked_que['losses']
                    # print("Flex:", tier_flex, rank_flex, wins_flex, loses_flex)
        return to_return
    except ApiError as err:
        # Wait 5 seconds and try again
        print("Summoner-v4 retry in 5 seconds")
        time.sleep(5)


def get_match_list_by_queue_id(region: str, puuid: str, queue_id: int = 0):
    """
    Takes region, players puuid and optionally queue type (default is all match queues), searches all matches in the
    last 180 days for that queue type, returns dict number of matches and match list in dict form
    "number_of_matches" and "match_list"
    """
    total_match_list = []
    starting_index = 0
    # Last 180 days only
    epoch_180_days_ago = int(time.time()) - 180 * 24 * 60 * 60
    try:
        while True:
            if queue_id == 0:
                match_list = api_connector.match.matchlist_by_puuid(region=region, puuid=puuid, count=100,
                                                                    start=starting_index, start_time=epoch_180_days_ago)
            else:
                match_list = api_connector.match.matchlist_by_puuid(region=region, puuid=puuid, count=100,
                                                                    start=starting_index, queue=queue_id,
                                                                    start_time=epoch_180_days_ago)
            total_match_list += match_list
            if len(match_list) >= 99 and starting_index < 900:
                # print(len(match_list), starting_index)
                starting_index += len(match_list)
            else:
                break
    except ApiError as err:
        print("Couldn't retrieve matchlist")
        # print(err)
        # return 1
        raise err

    dict_to_return = {
        'number_of_matches': len(total_match_list),
        'match_list': total_match_list
    }
    return dict_to_return


def get_games_played(region: str, puuid: str):
    """
    Takes region (EUROPE) and puuid, get match lists for different queue IDs.
    Returns dict:
    games_in_last_180_days
    normal_180_days
    draft_180_days
    aram_180_days
    ranked_solo_180_days
    ranked_flex_180_days
    other_180_days
    normal_draft_solo_flex_aram_list
    """
    all_games = get_match_list_by_queue_id(region, puuid)
    normal_games = get_match_list_by_queue_id(region, puuid, 430)
    draft_games = get_match_list_by_queue_id(region, puuid, 400)
    solo_duo_games = get_match_list_by_queue_id(region, puuid, 420)
    flex_games = get_match_list_by_queue_id(region, puuid, 440)
    aram_games = get_match_list_by_queue_id(region, puuid, 450)
    other_180_days = all_games['number_of_matches'] - normal_games['number_of_matches'] - \
                     draft_games['number_of_matches'] - aram_games['number_of_matches'] - \
                     solo_duo_games['number_of_matches'] - flex_games['number_of_matches']
    return{
        'games_in_last_180_days': all_games['number_of_matches'],
        'normal_180_days': normal_games['number_of_matches'],
        'draft_180_days': draft_games['number_of_matches'],
        'aram_180_days': aram_games['number_of_matches'],
        'ranked_solo_180_days': solo_duo_games['number_of_matches'],
        'ranked_flex_180_days': flex_games['number_of_matches'],
        'other_180_days': other_180_days,
        'normal_draft_solo_flex_aram_list': normal_games['match_list'] + draft_games['match_list'] +
                                            solo_duo_games['match_list'] + flex_games['match_list'] +
                                            aram_games['match_list']
    }


def get_player_info(server: str, puuid: str):
    """
    Takes server (ex. EUN1), and player puuid (default) or summoner name.
    Does following API requests:
    """
    try:
        summoner_info = get_summoner_v4_info(server, puuid)
        if not summoner_info:
            print("Summoner info not fetched, ending get_player_info function")
            return False
        league_info = get_league_v4_info(server, summoner_info['summoner_id'])
        region = api_regions[server]
        games_played = get_games_played(region, puuid)
    except ApiError as err:
        print("Get player info failed:")
        print(err)
        return False
    requests_timestamp = {'requests_timestamp': (int(time.time()))}
    merged_dict = {}
    merged_dict.update(requests_timestamp)
    merged_dict.update({'server': server})
    merged_dict.update(summoner_info)
    merged_dict.update(league_info)
    merged_dict.update(games_played)
    # print(merged_dict)
    return merged_dict


def insert_into_database(to_insert: dict):
    """
    Takes all values to insert to player_info table and tries to insert them. Returns True if inserted, tries again
    if not inserted
    """

    insert_query = """
        INSERT INTO [dbo].[player_info]
               ([requests_timestamp]
               ,[server]
               ,[summoner_name]
               ,[puuid]
               ,[account_id]
               ,[summoner_id]
               ,[profile_icon_id]
               ,[revision_date]
               ,[summoner_level]
               ,[tier_solo_duo]
               ,[rank_solo_duo]
               ,[wins_solo_duo]
               ,[loses_solo_duo]
               ,[tier_flex]
               ,[rank_flex]
               ,[wins_flex]
               ,[loses_flex]
               ,[games_in_last_180_days]
               ,[normal_180_days]
               ,[draft_180_days]
               ,[aram_180_days]
               ,[ranked_solo_180_days]
               ,[ranked_flex_180_days]
               ,[other_180_days])
         VALUES ({},'{}','{}','{}','{}','{}',{},{},{},'{}','{}',{},{},'{}','{}',{},{},{},{},{},{},{},{},{})
        """.format(
        to_insert['requests_timestamp'],
        to_insert['server'],
        to_insert['summoner_name'],
        to_insert['puuid'],
        to_insert['account_id'],
        to_insert['summoner_id'],
        to_insert['profile_icon_id'],
        to_insert['revision_date'],
        to_insert['summoner_level'],
        to_insert['tier_solo_duo'],
        to_insert['rank_solo_duo'],
        to_insert['wins_solo_duo'],
        to_insert['loses_solo_duo'],
        to_insert['tier_flex'],
        to_insert['rank_flex'],
        to_insert['wins_flex'],
        to_insert['loses_flex'],
        to_insert['games_in_last_180_days'],
        to_insert['normal_180_days'],
        to_insert['draft_180_days'],
        to_insert['aram_180_days'],
        to_insert['ranked_solo_180_days'],
        to_insert['ranked_flex_180_days'],
        to_insert['other_180_days']
    )
    while True:
        try:
            # db_connector = pyodbc.connect(connection_string, timeout=10)
            # cursor = db_connector.cursor()
            db_connector = pymssql.connect("dyplom.wwsi.edu.pl:50222", passwords['db_login'], passwords['db_password'],
                                           "INZ2022LoLCZ")
            cursor = db_connector.cursor(as_dict=True)
            cursor.execute(insert_query)
            db_connector.commit()
            db_connector.close()
            now = (datetime.now()).strftime("%H:%M:%S")
            print(now, ': "', to_insert['summoner_name'], '" inserted into database', sep="")
#         current_time = now.strftime("%H:%M:%S"))
            return True
        except Exception as err:
            print(err)
            print("Error inserting to database, retrying in 10 seconds")
            time.sleep(10)


def get_player_from_database(number_from_end: int = 1):
    """
    Get Nth player last inserted into database, returns dict with player's server and puuid if done, False if failed
    """
    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};' \
                        'SERVER=dyplom.wwsi.edu.pl,50222;DATABASE=INZ2022LoLCZ;' \
                        'UID=' + passwords['db_login'] + ';PWD=' + passwords['db_password'] + ";"
    select_query = """SELECT TOP ({}) [id_player_info],[requests_timestamp],[server],[summoner_name],[puuid],[account_id]
                      FROM [INZ2022LoLCZ].[dbo].[player_info]
                      ORDER BY id_player_info DESC """.format(number_from_end)
    try:
        # db_connector = pyodbc.connect(connection_string)
        # cursor = db_connector.cursor()
        db_connector = pymssql.connect("dyplom.wwsi.edu.pl:50222", passwords['db_login'], passwords['db_password'],
                                       "INZ2022LoLCZ")
        cursor = db_connector.cursor(as_dict=True)
        cursor.execute(select_query)
        row = cursor.fetchone()
        while row is not None:
            # print("db row")
            # print(row)
            last_player = row
            row = cursor.fetchone()
        # print(last_player)
        db_connector.commit()
        db_connector.close()

        return{
            'server': last_player['server'],
            'summoner_name': last_player['summoner_name'],
            'puuid': last_player['puuid']
        }
    except pymssql.Error as err:
        print("Couldn't get last player")
        print(err)
        return False


def fetch_new_player(server: str = "", puuid: str = "", match_list: list = []):
    """
    Fetch next player. If any of the arguments not given or an error arises during execution get last player from
    the database
    """
    try_number = 1
    while True:
        try:
            if puuid == "" or server == "" or match_list == [] or try_number > 1:
                print("#Fetch_new_players if clause")
                print("#puuid:", puuid)
                print("#server:", server)
                # print("#match_list:", match_list)
                last_player = get_player_from_database(try_number)
                # print("#last player", last_player)
                puuid = last_player['puuid']
                server = last_player['server']
                region = api_regions[server]
                match_list = get_games_played(region, puuid)['normal_draft_solo_flex_aram_list']
                # print(match_list)
            region = api_regions[server]
            seed_match = random.choice(match_list)
            # print(seed_match)
            match_info = api_connector.match.by_id(region, seed_match)
            participants = match_info['metadata']['participants']
            random_player_puuid = random.choice(participants)
            while random_player_puuid == puuid:
                random_player_puuid = random.choice(participants)
                # print("Dang it,", puuid, "again! Retry!")
            info = get_player_info(server, random_player_puuid)
            insert_into_database(info)
            return {"server": info['server'], "puuid": info['puuid'],
                    "match_list": info['normal_draft_solo_flex_aram_list']}
        except KeyboardInterrupt as keyboard:
            now = (datetime.now()).strftime("%H:%M:%S")
            print("Keyboard interrupt. Exiting.")
            exit()
        except IndexError as err:
            now = (datetime.now()).strftime("%H:%M:%S")
            print(err)
            print("{}: Couldn't get the player info, retrying in 120 seconds. Retry {}".format(now, try_number))
            try_number += 1
            time.sleep(120)
        except TypeError as err:
            now = (datetime.now()).strftime("%H:%M:%S")
            print(err)
            print("{}: Couldn't get the player info, retrying in 120 seconds. Retry {}".format(now, try_number))
            try_number += 1
            time.sleep(120)
        except ApiError as api_err:
            now = (datetime.now()).strftime("%H:%M:%S")
            print(api_err)
            print("{}: Couldn't get the player info, retrying in 120 seconds. Retry {}".format(now, try_number))
            try_number += 1
            time.sleep(120)
        # except Exception as err:
        #     print("Unknown error:")
        #     print(err)
        #     now = (datetime.now()).strftime("%H:%M:%S")
        #     print("{}: Waiting 10 minutes seconds and retrying. Retry {}".format(now, try_number))
        #     try_number += 1
        #     time.sleep(600)


latest_player = fetch_new_player()
while True:
    latest_player = fetch_new_player(latest_player['server'], latest_player['puuid'], latest_player['match_list'])
