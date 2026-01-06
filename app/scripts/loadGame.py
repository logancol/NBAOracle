from nba_api.stats.static import teams
import psycopg2
from nba_api.stats.library.parameters import Season
from nba_api.stats.library.parameters import SeasonType
from time import sleep
from dotenv import load_dotenv
import os
from nba_api.stats.endpoints import leaguegamefinder
from datetime import datetime

class GameLoader():
    def __init__(self, db_connection, update: bool):
        self.conn = db_connection
        self.cur = self.conn.cursor()
        self.team_abbreviations = [
            # current teams
            "ATL", "BOS", "BKN", "CHI", "CHA", "CLE", "DAL", "DEN",
            "DET", "GSW", "HOU", "IND", "LAC", "LAL", "MEM", "MIA",
            "MIL", "MIN", "NOP", "NYK", "OKC", "ORL", "PHI", "PHX",
            "POR", "SAC", "SAS", "TOR", "UTA", "WAS",

            # historical teams in play-by-play era
            "NJN",  # New Jersey Nets (pre-Brooklyn)
            "CHB",  # Original Charlotte Hornets before relocation
            "VAN",  # Vancouver Grizzlies
            "SEA"   # Seattle SuperSonics
        ]
        self.nba_teams = teams.get_teams()
        self.team_ids = [team['id'] for team in self.nba_teams]
        print(len(self.team_ids))
        self.update = update
        # maps team abbreviations to ids
        self.id_map = {}
        for team in self.nba_teams:
            self.id_map[team['abbreviation']] = team['id']
        self.id_map['NJN'] = 1 # assigning custom ids for older team abrevs in the pbp era to allow foreign contraint
        self.id_map['CHB'] = 2
        self.id_map['VAN'] = 3
        self.id_map['SEA'] = 4

    def load_regular_season_games(self):
        for id in self.team_ids:
            print(f'LOADING GAMES FOR TEAM {id}')
            if self.update:
                print('UPDATING GAMES FOR CURRENT SEASON')
                gamefinder_regular = leaguegamefinder.LeagueGameFinder(team_id_nullable=id, season_type_nullable="Regular Season", season_nullable='2025-26') 
            else:
                gamefinder_regular = leaguegamefinder.LeagueGameFinder(team_id_nullable=id, season_type_nullable="Regular Season")
            games = gamefinder_regular.get_data_frames()[0]
            for _, game in games.iterrows():
                # if the game includes a non play-by-play era team
                team_1 = game['TEAM_ABBREVIATION']
                team_2 = game['MATCHUP'][-3:]
                if team_1 == 'CHH':
                    team_1 = 'CHA'
                if team_2 == 'CHH':
                    team_2 = 'CHA'
                if team_1 not in self.team_abbreviations:
                    print(f'THIS IS WEIRD, game with unknown abbreviation : {team_1}')
                    continue
                if team_2 not in self.team_abbreviations:
                    print(f'THIS IS WEIRD, game with unknown abbreviation : {team_2}')
                    continue
                game_id = game['GAME_ID']
                season_id = game['SEASON_ID']
                home_team_id = -1
                away_team_id = -1
                if game['MATCHUP'][4] == '@': # primary team away
                    away_team_id = game['TEAM_ID']
                    home_team_id = self.id_map[team_2]
                else:
                    home_team_id = game['TEAM_ID']
                    away_team_id = self.id_map[team_2]
                game_date = datetime.strptime(game['GAME_DATE'], "%Y-%m-%d").date()
                season_type = "regular"
                self.cur.execute("INSERT INTO GAME (game_id, season_id, home_team_id, away_team_id, game_date, season_type) " \
                "Values (%s, %s, %s, %s, %s, %s) ON CONFLICT (game_id) DO NOTHING;", (game_id, season_id, home_team_id, away_team_id, game_date, season_type))

        gamefinder_playoff = leaguegamefinder.LeagueGameFinder(team_id_nullable=id, season_type_nullable="Playoffs")
        games = gamefinder_playoff.get_data_frames()[0]
        for _, game in games.iterrows():
            team_1 = game['TEAM_ABBREVIATION']
            team_2 = game['MATCHUP'][-3:]
            if team_1 == 'CHH':
                team_1 = 'CHA'
            if team_2 == 'CHH':
                team_2 = 'CHA'
            if team_1 not in self.team_abbreviations or team_2 not in self.team_abbreviations:
                continue
            game_id = game['GAME_ID']
            season_id = game['SEASON_ID']
            home_team_id = -1
            away_team_id = -1
            if game['MATCHUP'][4] == '@': # home team away
                away_team_id = game['TEAM_ID']
                home_team_id = self.id_map[team_2]
            else:
                home_team_id = game['TEAM_ID']
                away_team_id = self.id_map[team_2]
            game_date = datetime.strptime(game['GAME_DATE'], "%Y-%m-%d").date()
            season_type = "playoff"
            self.cur.execute("INSERT INTO GAME (game_id, season_id, home_team_id, away_team_id, game_date, season_type) " \
            "Values (%s, %s, %s, %s, %s, %s) ON CONFLICT (game_id) DO NOTHING;", (game_id, season_id, home_team_id, away_team_id, game_date, season_type))
        self.conn.commit()

def main():
    load_dotenv()
    DB_URL = os.getenv('DATABASE_URL')
    conn = psycopg2.connect(DB_URL)
    game_loader = GameLoader(db_connection=conn, update=False)
    game_loader.load_regular_season_games()

if __name__ == '__main__':
    main()