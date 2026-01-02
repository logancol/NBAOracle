import psycopg2
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.library.parameters import Season
from nba_api.stats.library.parameters import SeasonType
from nba_api.stats.endpoints import playbyplayv3
from datetime import timedelta
import isodate
import time
import re
import pandas as pd

def iso8601_to_sql_interval(duration: str) -> str:
    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?'
    match = re.match(pattern, duration)
    if not match:
        raise ValueError(f"Invalid ISO 8601 duration: {duration}")
    
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = float(match.group(3) or 0)

    sec_int = int(seconds)
    microsec = int((seconds - sec_int) * 1_000_000)
    interval_str = f"{hours} hours {minutes} minutes {sec_int} seconds {microsec} microseconds"
    
    return interval_str

# Get play by play from found games 

conn = psycopg2.connect(
    database="streamd",
    user="docker",
    password="docker",
    port=5431
)

# Open cursor to perform database operations
cur = conn.cursor()

cur.execute('SELECT game_id, season_type, season_id FROM Game;')
rows = cur.fetchall()
game_ids = [row[0] for row in rows]

dfs = []
for row in rows:
    df = (playbyplayv3.PlayByPlayV3(game_id = row[0]).get_data_frames()[0])
    df['season_type'] = row[1]
    df['season_id'] = row[2]
    time.sleep(2) # avoid hitting rate limit

# -- IN THE FUTURE, SET UP SUPPORT FOR SHOT CHART DETAIL ENDPOINT WHICH WILL GIVE COORDINATES AND MORE ACCURATE SHOT LOCATION INFORMATION

final_df = pd.concat(dfs, ignore_index=True)
for index, row in final_df.iterrows():
    # retooling with v3 endpoint its way better
    if row['isFieldGoal'] == 1:
        game_id = row['gameId']
        event_num = row['actionNumber']
        event_type = row['actionType']
        event_subtype = row['subType']
        season = row['season_id'] # must add manually
        season_type = row['season_type'] # must add manually
        period = row['period']
        clock = iso8601_to_sql_interval(row['clock'])
        posession_team_id = row['teamId']
        primary_player_id = row['personId']
        home_team_id = row['home_team_id']
        away_team_id = row['visiting_team_id']
        shot_x = row['xLegacy']
        shot_y = row['yLegacy']
        home_score = row['scoreHome']
        away_score = row['scoreAway']
        is_three = row['shotValue'] == 3
        shot_made = row['shotResult'] == 'Made'
        points = 

        
        








#cur.execute("INSERT INTO Games (game_id, season) VALUES (%s, %s)", (1929, 20002))
#cur.execute("SELECT * FROM Games")
#rows = cur.fetchall()
#conn.commit()
#print(rows)

# Query the databse
# docker exec -it streamd_db psql -U docker -d streamd lets you work with the db from command line