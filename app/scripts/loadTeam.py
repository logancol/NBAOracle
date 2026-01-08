import psycopg2
import os
import logging
import time
from dotenv import load_dotenv
from nba_api.stats.endpoints import TeamDetails
import sys

class TeamLoader:
    def __init__(self, db_conn):
        self.conn = db_conn
        self.cur = db_conn.cursor()

        # Working with team ids, abbreviations, and nicknames can be confusion, as there are instances with teams having the same id and abrev but different nicknames, etc. 
        # For now, I'm going to manage these personally since there's only in the range of 30-40 "teams" in the pbp era that I need to be concerned about 

        self.team_ids = [1610612766,1610612764,1610612759,1610612751,1610612739,1610612738,1610612758,
            1610612765,1610612747,1610612762,1610612749,1610612757,1610612761,1610612745,
            1610612754,1610612737,1610612741,1610612746,1610612742,1610612750,1610612756,
            1610612755,1610612752,1610612743,1610612753,1610612744,1610612763,1610612748,
            1610612760,1610612763,1610612740,1610612766,1610612740,1610612760,1610612751,
            1610612740]
        # abreviation to team id conversions for the play by play era
        self.abrev_id_map = {'ATL': 1610612737, 'BKN': 1610612751,'BOS': 1610612738,'CHA': 1610612766,'CHH': 1610612766,'CHI': 1610612741,'CLE': 1610612739,
                             'DAL': 1610612742,'DEN': 1610612743,'DET': 1610612765,'GSW': 1610612744,'HOU': 1610612745,'IND': 1610612754,'LAC': 1610612746,
                             'LAL': 1610612747,'MEM': 1610612763,'MIA': 1610612748,'MIL': 1610612749,'MIN': 1610612750,'NJN': 1610612751,'NOH': 1610612740,
                             'NOK': 1610612740,'NOP': 1610612740,'NYK': 1610612752,'OKC': 1610612760,'ORL': 1610612753,'PHI': 1610612755,'PHX': 1610612756,
                             'POR': 1610612757,'SAC': 1610612758,'SAS': 1610612759,'SEA': 1610612760,'TOR': 1610612761,'UTA': 1610612762,'VAN': 1610612763,'WAS': 1610612764}

        self.abrev_nickname_map = {'ATL': 'Hawks', 'BKN': 'Nets', 'BOS': 'Celtics', 'CHA': 'Hornets', 'CHH': 'Hornets', 'CHI': 'Bulls', 'CLE': 'Cavaliers',
                           'DAL': 'Mavericks', 'DEN': 'Nuggets', 'DET': 'Pistons', 'GSW': 'Warriors', 'HOU': 'Rockets', 'IND': 'Pacers', 'LAC': 'Clippers',
                           'LAL': 'Lakers', 'MEM': 'Grizzlies', 'MIA': 'Heat', 'MIL': 'Bucks', 'MIN': 'Timberwolves', 'NJN': 'Nets', 'NOH': 'Hornets',
                           'NOK': 'Hornets', 'NOP': 'Pelicans', 'NYK': 'Knicks', 'OKC': 'Thunder', 'ORL': 'Magic', 'PHI': '76ers', 'PHX': 'Suns',
                           'POR': 'Trail Blazers', 'SAC': 'Kings', 'SAS': 'Spurs', 'SEA': 'SuperSonics', 'TOR': 'Raptors', 'UTA': 'Jazz', 'VAN': 'Grizzlies',
                           'WAS': 'Wizards'}
        
        self.team_ids = list(set(self.team_ids))
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        log_formatter = logging.Formatter("%(asctime)s [%(processName)s: %(process)d] [%(threadName)s: %(thread)d] [%(levelname)s] %(name)s: %(message)s")
        stream_handler.setFormatter(log_formatter)
        self.logger.addHandler(stream_handler)

    def load_historical_teams(self):
        success = True
        for id in self.team_ids:
            try:
                df = TeamDetails(team_id = id).get_data_frames()
            except Exception:
                self.logger.error(f"====== ISSUE WITH NBA API GETTING TEAM HISTORICAL TEAM DETAILS FOR TEAM WITH ID: {id} ======")
                success = False
                break
            historical = df[1]
            for _, row in historical.iterrows():
                self.logger.info(f"====== STORING HISTORICAL TEAM INDEX FOR TEAM WITH ID: {id} ======")
                self.logger.info(f"====== {id}, {row['CITY']}, {row['NICKNAME']}, {row['YEARFOUNDED']}, {row['YEARACTIVETILL']}")
                current_iteration = False
                if row['YEARACTIVETILL'] == historical['YEARACTIVETILL'].max():
                    current_iteration = True
                try:
                    self.cur.execute("INSERT INTO historical_team_index (id, current_iteration, city, nickname, year_founded, year_active_til) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;", 
                            (id, current_iteration, row['CITY'], row['NICKNAME'], row['YEARFOUNDED'], row['YEARACTIVETILL']))
                except Exception:
                    self.logger.error(f"====== ERROR STORING HISTORICAL TEAM INDEX FOR TEAM WITH ID: {id} ======")
                    success = False
                    break
            if not success: 
                break
            time.sleep(.2)
        if success:
            self.conn.commit()
            self.logger.info(f"====== COMMITED CHANGES TO DATABASE ======")

    def load_modern_teams(self):
        success = True
        for abrev in self.abrev_id_map.keys():
            self.logger.info(f"====== STORING MODERN TEAM INDEX FOR TEAM WITH ABBREVIATION: {abrev} ======")
            id = self.abrev_id_map[abrev]
            nickname = self.abrev_nickname_map[abrev]
            try:
                self.cur.execute("INSERT INTO modern_team_index (id, abrev, nickname) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (id, abrev, nickname))
            except Exception as e:
                self.logger.error(f"====== ERROR STORING MODERN TEAM INDEX FOR TEAM WITH ABBREVIATION: {abrev} ERROR: {e} ======")
                success = False
                break
        if success: 
            self.conn.commit()
            self.logger.info(f"====== COMMITED CHANGES TO DATABASE ======")
    
    def close_connection(self):
        self.conn.close()
            

def main():
    load_dotenv()
    DB_URL = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(DB_URL)
    loader = TeamLoader(conn)
    loader.load_historical_teams()
    loader.load_modern_teams()
    loader.close_connection()

if __name__ == '__main__':
    main()