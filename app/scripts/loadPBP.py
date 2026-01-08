import psycopg2
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.library.parameters import Season
from nba_api.stats.library.parameters import SeasonType
from nba_api.live.nba.endpoints import PlayByPlay
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

class PBPDataLoader:
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

    # --- Remove accents from player name utility ---
    def remove_accents(self, s: str) -> str: 
        if not isinstance(s, str):
            return s
        return (
            unicodedata.normalize("NFKD", s)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
            
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
    
    # --- Loading play by play data for shots for season in season_ids, batching for efficiency ---
    def insert_pbp_data_batch(self, data: list, batch_size: int):
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


    def load_pbp_data(self):    
        if self.update:
            season_ids = [22025]
        else:
            season_ids = [21996, 41996, 21997, 41997, 21998, 41998, 21999, 41999, 22000, 42000, 22001, 42001, 
                          22002, 42002, 22003, 42003, 22004, 42004, 22005, 42005, 22006, 42006, 22007, 42007, 
                          22008, 42008, 22009, 42009, 22010, 42010, 22011, 42011, 22012, 42012, 22013, 42013, 
                          22014, 42014, 22015, 42015, 22016, 42016, 22017, 42017, 22018, 42018, 22019, 42019, 
                          22020, 42020, 22021, 42021, 22022, 42022, 22023, 42023, 22024, 42024, 22025]

        # Fetch games logged in the database
        try:
            self.cur.execute('SELECT id, season_type, season_id, home_team_id, away_team_id FROM Game;') 
            rows = self.cur.fetchall()
        except psycopg2.Error as e:
            self.logger.error(f"====== ERROR FETCHING GAME INFO: {e} ======")

        relevant_games = [row for row in rows if row[2] in season_ids]
        num_games = len(relevant_games)
        count = 0

        # getting pbp dfs for each fetched game, processing, inserting into dataframe
        for row in relevant_games:
            count += 1
            self.logger.info(f'====== FETCHING PBP INFO FOR GAME: {count} OF {num_games} ======')
            game_id = str(row[0]).zfill(10)
            try:
                pbp = PlayByPlay.player_stats(game_id=game_id)
                df = pd.DataFrame(pbp.actions.get_dict())
            except Exception as e:
                self.logger.error(f'======= ERROR FETCHING PBP INFO FOR GAME {game_id} ======')
            game_id = int(game_id)
            df['season_type'] = row[1] # append season type string manually
            df['season_id'] = row[2] # append season id manually
            df['home_team_id'] = row[3] 
            df['away_team_id'] = row[4]
            # lets try just logging this data here
            
            for _, event in df.iterrows():
                # non-conditional on event type
                event_num = event['actionNumber']
                event_type = event['actionType']
                event_subtype = event['subType']
                home_score = event['scoreHome']
                away_score = event['scoreAway']
                period = event['period']
                clock = self.iso8601_to_sql_interval(event['clock'])
                home_team_id = row[3]
                away_team_id = row[4]
                possesion_team_id = event['possession']
                is_over_time = False
                if period > 4:
                    is_overtime = True

                # conditional on event
                shooter_id = None
                assister_id = None
                jump_ball_winner_id = None
                jump_ball_loser_id = None
                jump_ball_recovered_id = None
                rebounder_id = None
                foul_drawn_id = None
                fouler_id = None
                stealer_id = None
                blocker_id = None
                sub_in_id = None
                sub_out_id = None
                turnover_id = None
                foul_is_technical = None
                offensive_rebound = None
                side = None
                descriptor = None
                area = None
                area_detail = None
                shot_distance = None
                shot_made = None
                shot_value = None
                shot_x = None            
                shot_y = None

                # filling conditional on event type
                if event['actionType'] == 'freethrow':
                    shot_value = 1
                    shooter_id = event['personId']
                    shot_made = True if event['shotResult'] == 'Made' else False
                elif event['isFieldGoal'] == 1: # nba_api does not properly support logging made heaves at the moment, I should do this an an open source contribution, ignoring for now
                    if event['actionType'] == '2pt':
                        shot_value = 2
                    else:
                        shot_value = 3
                    side = event['side']
                    descriptor = event['descriptor']
                    shot_x = event['x']
                    shot_y = event['y']
                    area = event['area']
                    area_detail = event['areaDetail']
                    shot_distance = event['shotDistance']
                    shooter_id = event['personId']
                    assister_id = int(event['assistPersonId'])
                    shot_made = True if event['shotResult'] == 'Made' else False
                    if not shot_made:
                        if event['blocker_id']:
                            blocker_id = int(event['blockPersonId'])
                elif event['actionType'] == 'jumpball':
                    jump_ball_recovered_id = int(event['jumpBallRecoverdPersonId'])
                    jump_ball_loser_id = int(event['jumpBallLostPersonId'])
                    jump_ball_winner_id = int(event['jumpBallWonPersonId'])
                elif event['actionType'] == 'turnover':
                    turnover_id = event['personId']
                    area = event['area']
                    area_detail = event['areaDetail']
                    if event['stealPersonId']:
                        stealer_id = int(event['stealPersonId'])
                elif event['actionType'] == 'foul':
                    foul_is_technical = event['subType'] == 'technical'
                    foul_drawn_id = event['foulDrawnPersonId']
                    fouler_id = event['personId']
                elif event['actionType'] == 'substitution':
                    if event['subType'] == 'out':
                        sub_out_id = event['personId']
                    if event['subType'] == 'in':
                        sub_in_id = event['personId']
                elif event['actionType'] == 'rebound':
                    rebounder_id = event['personId']
                    offensive_rebound = event['subType'] == 'offensive'



        

def main():
    load_dotenv() 
    DB_URL = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(DB_URL)
    data_loader = PBPDataLoader(conn, update=True)
    data_loader.load_pbp_data()

if __name__ == '__main__':
    main()