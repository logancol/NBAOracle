import psycopg2
from nba_api.stats.static import players
from app.core.config import settings
import pandas as pd

DB_URL = settings.DATABASE_URL
conn = psycopg2.connect(DB_URL)
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