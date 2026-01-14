# initial non-update loading for all data
from scripts.loadPlayer import PlayerLoader
from scripts.loadPBP import PBPDataLoader
import psycopg
from app.core.config import settings
from scripts.loadGame import GameLoader
from scripts.loadTeam import TeamLoader

# -> update player index -> update game data -> update play by play data
def main():
    DB_URL = settings.DATABASE_URL
    with psycopg.connect(DB_URL) as conn:
        with conn.transaction():
            loader = TeamLoader(conn)
            with conn.cursor() as cur:
                loader.load_historical_teams(cur)
                loader.load_modern_teams(cur)

    with psycopg.connect(DB_URL) as conn:
        with conn.transaction():
            loader = PlayerLoader(conn)
            with conn.cursor() as cur:
                loader.load_player_index(cur)

    with psycopg.connect(DB_URL) as conn:
        with conn.transaction():
            game_loader = GameLoader(conn, update=False)
            game_loader.load_games()

    with psycopg.connect(DB_URL) as conn:
        data_loader = PBPDataLoader(conn, update=False)
        data_loader.load_pbp_data()


if __name__ == "__main__":
    main()