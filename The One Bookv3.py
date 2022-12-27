#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import time
import pandas as pd
import numpy as np
import json


# In[ ]:


api_key = ""
region = "EUROPE"
region2 = "euw1"
summoners_list = ['Qyntius', 'Bombirah', 'Bombira', 'Klaawi','Douglas Jay', 'Cruzzy', 'Ronelifan', 'M I Z E R']
count = 100
puuid_list = []
match_list = []


# In[ ]:


def get_summoner(region, name, api_key):
    api_url = ('https://'+
               region +
               '.api.riotgames.com/lol/summoner/v4/summoners/by-name/' +
               name +
               '?api_key=' +
               api_key
              )
    resp = requests.get(api_url)
    data = resp.json()
    return data


# In[ ]:


def get_matches(region, puuid, count, api_key):
    api_url = (
        "https://" +
        region +
        ".api.riotgames.com/lol/match/v5/matches/by-puuid/" +
        puuid +
        "/ids" +
        "?type=ranked&" +
        "start=0&" +
        "count=" +
        str(count) + 
        "&api_key=" +
        api_key
    )
    while True:
        resp = requests.get(api_url)

        if resp.status_code == 429:
            print("sleeping")
            time.sleep(10)
            continue 
        
        return resp.json()


# In[ ]:


def get_details(region, match_id, api_key):
    api_url = (
        "https://" +
        region +
        ".api.riotgames.com/lol/match/v5/matches/" +
        match_id +
        "?api_key=" +
        api_key
    )
    
    while True:
        resp = requests.get(api_url)

        if resp.status_code == 429:
            print("sleeping")
            time.sleep(10)
            continue
    
        data = resp.json()
        return data


# In[ ]:


def get_timeline(region, match_id, api_key):
    api_url = (
        "https://" +
        region +
        ".api.riotgames.com/lol/match/v5/matches/" +
        match_id +
        "/timeline" +
        "?api_key=" +
        api_key
    )
    
    while True:
        resp = requests.get(api_url)

        if resp.status_code == 429:
            print("sleeping")
            time.sleep(10)
            continue
    
        data = resp.json()
        return data


# In[ ]:


def tl_frame(frame, part):
    timeline_record = pd.json_normalize(tldetails['info']['frames'][frame]['participantFrames'][part])
    return timeline_record


# In[ ]:


def export_data(name, name_list):
    exporting = pd.concat(name_list)
    return exporting.to_csv(name+".csv", index = False)


# In[ ]:


keepplayer = [ 'participantId',
'assists', 
'champLevel',
'championName',
'deaths',
'goldEarned',
'kills',
'teamId',
'teamPosition',
'totalDamageDealtToChampions',
'totalDamageShieldedOnTeammates',
'totalDamageTaken',
'totalHeal',
'totalHealsOnTeammates',
'totalMinionsKilled',
'totalTimeCCDealt',
'win',
'match_id',
'summonerName',
'enemyteamId']

keepteam = ['teamId']

keepgame = ['gameCreation',
'gameDuration',
'queueId',
'match_id',
]

keeptimeline = ['jungleMinionsKilled',
'level',
'minionsKilled',
'participantId',
'timeEnemySpentControlled',
'totalGold',
'xp',
'damageStats.totalDamageDoneToChampions',
'damageStats.totalDamageTaken',
'position.x',
'position.y',
'frame',
'match_id']

eventkeep = ['participantId',
'event',
'assistingParticipantIds',
'victimId',
'position.x',
'position.y',
'teamId',
'frame',
'match_id',
'details',
'timestamp']

assistkeep = ['participantId',
'event',
'assistingParticipantIds',
'victimId',
'position.x',
'position.y',
'teamId',
'frame',
'match_id',
'details',
'timestamp']

eventtype = ['BUILDING_KILL',
'ELITE_MONSTER_KILL',
'CHAMPION_KILL']


# In[ ]:


#requesting puuid's
for player in summoners_list:
    summ = get_summoner(region2, player, api_key)
    summ = summ['puuid']
    puuid_list.append(summ)


# In[ ]:


#request match_list
for user in puuid_list:
    match = get_matches(region, user, count, api_key)
    match_list.append(match)


# In[ ]:


#cleaning match_list
flat_list = [item for sublist in match_list for item in sublist]
matches = [*set(flat_list)]


# In[ ]:


matches


# In[ ]:


#lists to store created details DataFrames
games_list = []
teams_list = []
players_list = []

# lists to store created timeline DataFrames
timeline_list = []
event_list = []

#participant list to loop through for timelines
part_list = ["1","2","3","4","5","6","7","8","9","10"]


# In[ ]:


for match_id in matches:
    try:
        #requesting match data & timeline data
        details = get_details(region, match_id, api_key)
        tldetails = get_timeline(region, match_id, api_key)

        #define current matchId
        current_matchid = details['metadata']['matchId']

        #create DataFrame for teams info
        teams = pd.json_normalize(details['info']['teams'])
        teams["match_id"] = (current_matchid,)*2
        teams = teams.replace({'teamId': {100:'blue', 200:'red'}})
        teams['teamId'] = teams['teamId'].astype(str) + teams['match_id'].astype(str)
        teams = teams.loc[:,keepteam]
        teams_list.append(teams)

        #create DataFrame for games info 
        games = pd.json_normalize(details['info'])
        games['match_id'] = current_matchid
        games['gameCreation'] = pd.to_datetime(games['gameCreation'],unit='ms') 
        games['gameDuration'] = games['gameDuration'].div(60)
        games = games.loc[:,keepgame]
        games_list.append(games)

        #create Dataframe for players info
        players = pd.json_normalize(details['info']['participants'])
        players["match_id"] = (current_matchid,)*10
        players['enemyteamId'] = players.loc[:, 'teamId']
        players = players.replace({'enemyteamId': {200:'blue', 100:'red'}})
        players = players.replace({'teamId': {100:'blue', 200:'red'}})
        players['teamId'] = players['teamId'].astype(str) + players['match_id'].astype(str)
        players['enemyteamId'] = players['enemyteamId'].astype(str) + players['match_id'].astype(str)
        players['participantId'] = players['participantId'].astype(str) + players['match_id'].astype(str)
        players['win'] = players['win'].astype(int)
        players = players.loc[:,keepplayer]
        players_list.append(players)

        #create Dataframe for timeline info
        part_list = ["1","2","3","4","5","6","7","8","9","10"]
        gamelength = range(int(round(games['gameDuration'].values[0])))


        for y in gamelength:
            #create events table
            events = pd.json_normalize(tldetails['info']['frames'][y]['events'])
            events['frame'] = y
            events["match_id"] = current_matchid
            event_list.append(events)

            #create timeline table for each participant
            for part in part_list:
                timeline = tl_frame(y, part)
                timeline['frame'] = y
                timeline["match_id"] = (current_matchid)
                timeline = timeline.loc[:,keeptimeline]
                timeline['participantId'] = timeline['participantId'].astype(str) + timeline['match_id'].astype(str)
                timeline['totalcs'] = timeline['jungleMinionsKilled']+ timeline['minionsKilled']
                timeline_list.append(timeline)
    except KeyError:
        continue


# In[ ]:


if details['gameDuration'] == 0:
    continue
else:
    


# In[ ]:





# In[ ]:





# In[ ]:


events = pd.concat(event_list)
events = events[events.type.isin(eventtype)]
events.fillna('', inplace=True)
events = events.astype(str)

events['participantId'] = events[['participantId', 'killerId']].apply(lambda x: ''.join(x), axis=1)
events['details'] = events[['monsterSubType', 'towerType']].apply(lambda x: ''.join(x), axis=1)
events['details'] = np.where(events.monsterType == 'RIFTHERALD', 'RIFTHERALD', events.details)
events['details'] = np.where(events.monsterType == 'BARON_NASHOR', 'BARON_NASHOR', events.details)
events['details'] = np.where(events.buildingType == 'INHIBITOR_BUILDING', 'INHIBITOR', events.details)
events['type'] = np.where(events.monsterType == 'DRAGON', 'DRAGON_KILL', events.type)

events['details'] = np.where((
    events['type'] == 'CHAMPION_KILL') & (
    events['assistingParticipantIds'] == ''),
    'SOLO_KILL', events['details'])

events = events.replace({'teamId': {'200.0':'blue', '100.0':'red'}})
events = events.replace({'killerTeamId': {'100.0':'blue', '200.0':'red'}})
events['teamId'] = events[['teamId', 'killerTeamId']].apply(lambda x: ''.join(x), axis=1)

events = events.rename(columns={"type": "event"})
events['participantId'] = events['participantId'].replace(r'\.0$', '', regex=True)
events['victimId'] = events['victimId'].replace(r'\.0$', '', regex=True)
events['assistingParticipantIds'] = events['assistingParticipantIds'].str.strip('[]')
events = events.loc[:,eventkeep]


# In[ ]:


#10 = 4
events['assistingParticipantIds'].str.len().max()


# In[ ]:


len('1, 2, 3, 4, 5, 6, 7, 8, 9, 10')


# In[ ]:


assist_events = events.copy()

assist_events[['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H','I']] = assist_events['assistingParticipantIds'].str.split(',', expand=True)

assist_events = assist_events.melt(
    id_vars = ['event', 'assistingParticipantIds',
              'victimId', 'position.x', 'position.y', 'teamId', 
              'frame', 'match_id', 'details', 'timestamp'],
    value_vars = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H','I'],
    value_name = 'participantId')

assist_events.fillna('', inplace=True)
assist_events['participantId'].replace('', np.nan, inplace=True)
assist_events.dropna(subset=['participantId'], inplace=True)
assist_events = assist_events.replace({'event': {'CHAMPION_KILL':'CHAMPION_ASSIST', 'BUILDING_KILL':'BUILDING_ASSIST',
                                                   'ELITE_MONSTER_KILL':'ELITE_MONSTER_ASSIST', 'DRAGON_KILL':'DRAGON_ASSIST'}})
assist_events = assist_events.loc[:,assistkeep]


# In[ ]:


assist_events


# In[ ]:


death_events = events.copy()
death_events.rename(columns ={'participantId':'participantId2'}, inplace=True)
death_events.rename(columns ={'victimId':'participantId'}, inplace=True)
death_events.rename(columns ={'participantId2':'victimId'}, inplace=True)
death_events['participantId'].replace('', np.nan, inplace=True)
death_events.dropna(subset=['participantId'], inplace=True)
death_events = death_events.replace({'event': {'CHAMPION_KILL':'CHAMPION_DEATH'}})

finalevent_list = [events, assist_events, death_events]
finalevent = pd.concat(finalevent_list)
finalevent['participantId'] = finalevent['participantId'] + finalevent['match_id']
finalevent['victimId'] = finalevent['victimId'] + finalevent['match_id']
finalevent['teamId'] = finalevent['teamId'] + finalevent['match_id']
finalevent = finalevent.drop(['match_id'], axis = 1)


# In[ ]:


export_data("teams.csv",teams_list)
export_data("games.csv",games_list)
export_data("players",players_list)
export_data("timelines",timeline_list)
finalevent.to_csv("events.csv", index = False)


# In[ ]:





# In[ ]:





# In[ ]:




