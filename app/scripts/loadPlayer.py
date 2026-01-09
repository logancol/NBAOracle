import psycopg2
from nba_api.stats.static import teams
from nba_api.stats.static import players
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.library.parameters import Season
from nba_api.stats.library.parameters import SeasonType
from nba_api.stats.endpoints import playerindex
import time
import re
import pandas as pd

conn = psycopg2.connect(
    database="streamd",
    user="docker",
    password="docker",
    port=5431
)
cur = conn.cursor()

all_players = players._get_players()
for player in all_players:
    id = player['id']
    full_name = player['full_name']
    first_name = player['first_name']
    last_name = player['last_name']
    is_active = player['is_active']
    cur.execute("INSERT INTO PLAYER (id, full_name, first_name, last_name, is_active) VALUES (%s, %s, %s, %s, %s)", (id, full_name, first_name, last_name, is_active))

conn.commit()