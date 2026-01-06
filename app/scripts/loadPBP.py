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
from dotenv import load_dotenv
import sys
import pandas as pd
import os
import unicodedata
import logging

class PBPShotDataLoader:
    # --- Configure db connection and logging ---
    def __init__(self, db_connection, update: bool):
        self.conn = db_connection
        self.cur = db_connection.cursor()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        log_formatter = logging.Formatter("%(asctime)s [%(processName)s: %(process)d] [%(threadName)s: %(thread)d] [%(levelname)s] %(name)s: %(message)s")
        stream_handler.setFormatter(log_formatter)
        self.logger.addHandler(stream_handler)
        self.update = update

    # --- Remove accents from player name ---
    def remove_accents(self, s: str) -> str: 
        if not isinstance(s, str):
            return s
        return (
            unicodedata.normalize("NFKD", s)
            .encode("ascii", "ignore")
            .decode("ascii")
        )

    # --- Parse shot description to gather shot assister ---
    def parse_description_assist(self, game: pd.DataFrame, description: str, teamId: int) -> str:

        # Gather assister name from shot description
        assister = ""
        match = re.search(r'\(([^)]+?)\s\d+\sAST\)', description)
        if match:
            assister = match.group(1)
        else:
            return None
        
        # Look in current game df for the id associated with the normalized name pulled from the shot description
        player_name_cond = game['playerName'].apply(self.remove_accents) == assister
        player_name_I_cond = game['playerNameI'].apply(self.remove_accents) == assister
        team_id_cond = game['teamId'] == teamId
        
        # Use name conditions to find personId for the assister
        combined = (player_name_cond | player_name_I_cond) & team_id_cond
        filtered = game[combined]
        assister_id = None
        if not filtered.empty:
            assister_id = filtered.iloc[0]['personId']
        else:
            assister_id = None
        return assister_id
            
    # --- Helper: Convert the iso8601 timestamps (shot clock) to intervals for storage in postgres db
    def iso8601_to_sql_interval(self, duration: str) -> str:
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
    
    # --- Loading play by play data for shots for season in season_ids ---
    def insert_pbp_shot_data_batch(self, data: list, batch_size: int):
        for start in range(0, len(data), batch_size):
            end = start + batch_size
            batch = data[start:end]
            self.logger.info(f"====== INSERTING PBP SHOT DATA BATCH {start // batch_size} OF {len(data) // batch_size} ========")
            args = ','.join(self.cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", i).decode('utf-8')
                for i in batch)
            try:        
                self.cur.execute("INSERT INTO pbp_raw_event_shots (game_id, event_num, event_type, event_subtype, season, home_score, away_score, season_type, " \
                "period, clock, home_team_id, away_team_id, possession_team_id, primary_player_id, shot_x, shot_y, assister_id, is_three, shot_made, points) VALUES "
                + (args) + " ON CONFLICT DO NOTHING;")
            except psycopg2.Error as e:
                self.logger.error(f'====== PBP STORAGE ERROR: {e} BATCH #: {start} ======')
        self.conn.commit()
        self.logger.info("====== SUCESSFULLY INSERTED BATCHED PBP DATA ======")


    def load_pbp_shot_data(self):    
        if self.update:
            season_ids = [22025]
        else:
            season_ids = [21996, 41996, 21997, 41997, 21998, 41998, 21999, 41999, 22000, 42000, 22001, 42001, 
                          22002, 42002, 22003, 42003, 22004, 42004, 22005, 42005, 22006, 42006, 22007, 42007, 
                          22008, 42008, 22009, 42009, 22010, 42010, 22011, 42011, 22012, 42012, 22013, 42013, 
                          22014, 42014, 22015, 42015, 22016, 42016, 22017, 42017, 22018, 42018, 22019, 42019, 
                          22020, 42020, 22021, 42021, 22022, 42022, 22023, 42023, 22024, 42024, 22025]

        # Fetch games logged in the database
        self.cur.execute('SELECT game_id, season_type, season_id, home_team_id, away_team_id FROM Game;') 
        rows = self.cur.fetchall()

        # game ids for seasons in season_ids
        relevant_games = [row for row in rows if row[2] in season_ids]
        num_games = len(relevant_games)
        count = 0

        # storing pbp dfs for each fetched game
        dfs = []
        for row in relevant_games:
            count += 1
            self.logger.info(f'====== FETCHING PBP INFO FOR GAME: {count} OF {num_games} ======')
            game_id = str(row[0]).zfill(10)
            try:
                df = playbyplayv3.PlayByPlayV3(game_id=game_id).get_data_frames()[0]
            except Exception as e:
                self.logger.error(f'======= ERROR FETCHING PBP INFO FOR GAME {game_id} ======')
            df['season_type'] = row[1] # append season type string manually
            df['season_id'] = row[2] # append season id manually
            df['home_team_id'] = row[3] 
            df['away_team_id'] = row[4]
            time.sleep(0.2) # adjust to avoid rate limiting
            dfs.append(df)

        # concatenating pbp data for each game
        final_df = pd.concat(dfs, ignore_index=True)
        
        # total pbp events to log
        total = len(final_df.index)
        count = 0
        shot_data = []
        for _ , row in final_df.iterrows():
            self.logger.info(f"====== PROCESSING EVENT {_} OF {total} ======")
            count += 1
            if row['isFieldGoal'] == 1:
                assister_id = None
                game_id = row['gameId']
                event_num = row['actionNumber']
                event_type = row['actionType']
                event_subtype = row['subType']
                season = row['season_id']
                season_type = row['season_type']
                period = row['period']
                clock = self.iso8601_to_sql_interval(row['clock'])
                possession_team_id = row['teamId']
                primary_player_id = row['personId']
                home_team_id = row['home_team_id']
                away_team_id = row['away_team_id']
                shot_x = row['xLegacy']
                shot_y = row['yLegacy']
                home_score = None if row['scoreHome'] == '' else int(row['scoreHome'])
                away_score = None if row['scoreAway'] == '' else int(row['scoreAway'])
                is_three = row['shotValue'] == 3
                shot_made = row['shotResult'] == 'Made'
                points = row['shotValue'] if row['shotResult'] == 'Made' else 0
                if row['shotResult'] == 'Made':
                    sub = final_df[final_df['gameId'] == game_id][['playerName', 'playerNameI', 'teamId', 'personId']].drop_duplicates()
                    result = self.parse_description_assist(sub, row['description'], row['teamId'])
                    if result:
                        assister_id = int(result)
                event = [game_id, event_num, event_type, event_subtype, season, home_score, away_score, 
                         season_type, period, clock, home_team_id, away_team_id, possession_team_id, primary_player_id, 
                         shot_x, shot_y, assister_id, is_three, shot_made, points]
                shot_data.append(event)
        self.insert_pbp_shot_data_batch(shot_data, batch_size=1000)           

def main():
    load_dotenv() 
    DB_URL = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(DB_URL)
    shot_data_loader = PBPShotDataLoader(conn, update=True)
    shot_data_loader.load_pbp_shot_data()

if __name__ == '__main__':
    main()