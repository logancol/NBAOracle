CREATE TABLE Player
(
    player_id INT PRIMARY KEY,
    player_full_name VARCHAR(256),
    player_first_name VARCHAR(256),
    player_last_name VARCHAR(256),
    player_is_active BOOLEAN
);

CREATE TABLE Team
(
    team_id INT PRIMARY KEY,
    team_full_name VARCHAR(256),
    team_abbreviation VARCHAR(256),
    team_nickname VARCHAR(256),
    team_city VARCHAR(256)
);

CREATE TABLE Game
(
    game_id INT PRIMARY KEY,
    season_id INT,
    home_team_id INT REFERENCES Team(team_id),
    away_team_id INT REFERENCES Team(team_id),
    game_date DATE,
    season_type TEXT
);

CREATE TABLE pbp_raw_events (
  -- Identity
  game_id            BIGINT NOT NULL REFERENCES Game(game_id),
  event_num          INTEGER NOT NULL,        -- unique within game
  event_type         TEXT NOT NULL,            -- SHOT, REBOUND, FOUL, TURNOVER, SUB, etc
  event_subtype      TEXT,                     -- JUMPER, LAYUP, STEAL, OFF_FOUL, etc

  -- Game context
  season             INTEGER NOT NULL,
  season_type        TEXT NOT NULL,            -- REGULAR, PLAYOFFS
  period             INTEGER NOT NULL,
  clock              INTERVAL NOT NULL,        -- time remaining in period
  home_team_id       INTEGER REFERENCES Team(team_id),
  away_team_id       INTEGER REFERENCES Team(team_id),
  possession_team_id INTEGER,

  -- Primary actors (nullable by event_type)
  primary_player_id  INTEGER REFERENCES Player(player_id),                  -- shooter, fouler, turnover committer
  secondary_player_id INTEGER,                 -- assister, blocker, stealer
  tertiary_player_id INTEGER,                  -- sometimes used (e.g. fouled player)

  -- Explicit role columns (redundant but useful)
  passer_id          INTEGER,
  shooter_id         INTEGER,
  scorer_id          INTEGER,
  rebounder_id       INTEGER,
  blocker_id         INTEGER,
  fouler_id          INTEGER,
  fouled_id          INTEGER,
  turnover_player_id INTEGER,
  steal_player_id    INTEGER,
  assister_id        INTEGER,

  -- Shot context
  shot_type          TEXT,                     -- JUMPER, LAYUP, DUNK, FT
  shot_x             INTEGER,                  
  shot_y             INTEGER,
  shot_zone          TEXT,
  is_three           BOOLEAN,
  is_assisted        BOOLEAN,
  shot_made          BOOLEAN,
  points             INTEGER,

  -- Rebound context
  rebound_type       TEXT,                     -- OFF, DEF, DEADBALL

  -- Foul context
  foul_type          TEXT,                     -- SHOOTING, OFFENSIVE, TECH
  free_throws_awarded INTEGER,

  -- Turnover context
  turnover_type      TEXT,                     -- BAD_PASS, TRAVEL, OUT_OF_BOUNDS

  -- Substitution context
  player_in_id       INTEGER,
  player_out_id      INTEGER,

  -- Score context
  home_score         INTEGER,
  away_score         INTEGER,
  score_margin       INTEGER,

  -- Metadata
  team_id            INTEGER,
  event_description  TEXT,
  source_event_id    TEXT,
  created_at         TIMESTAMP DEFAULT now(),

  PRIMARY KEY (game_id, event_num)
);

CREATE TABLE pbp_raw_event_shots (
  -- Identity
  game_id            BIGINT NOT NULL REFERENCES Game(game_id),
  event_num          INTEGER NOT NULL,        -- unique within game
  event_type         TEXT NOT NULL,            -- SHOT, REBOUND, FOUL, TURNOVER, SUB, etc
  event_subtype      TEXT,                     -- JUMPER, LAYUP, STEAL, OFF_FOUL, etc

  -- Game context
  season             INTEGER NOT NULL,
  season_type        TEXT NOT NULL,            -- REGULAR, PLAYOFFS
  period             INTEGER NOT NULL,
  clock              INTERVAL NOT NULL,        -- time remaining in period
  home_team_id       INTEGER REFERENCES Team(team_id),
  away_team_id       INTEGER REFERENCES Team(team_id),
  possession_team_id INTEGER,

  -- Primary actors (nullable by event_type)
  primary_player_id  INTEGER REFERENCES Player(player_id),                  -- shooter, fouler, turnover committer
  secondary_player_id INTEGER,                 -- assister, blocker, stealer
  tertiary_player_id INTEGER,                  -- sometimes used (e.g. fouled player)

  -- Explicit role columns (redundant but useful)
  passer_id          INTEGER,
  shooter_id         INTEGER,
  scorer_id          INTEGER,
  rebounder_id       INTEGER,
  blocker_id         INTEGER,
  fouler_id          INTEGER,
  fouled_id          INTEGER,
  turnover_player_id INTEGER,
  steal_player_id    INTEGER,
  assister_id        INTEGER,

  -- Shot context
  shot_type          TEXT,                     -- JUMPER, LAYUP, DUNK, FT
  shot_x             INTEGER,                  
  shot_y             INTEGER,
  shot_zone          TEXT,
  is_three           BOOLEAN,
  is_assisted        BOOLEAN,
  shot_made          BOOLEAN,
  points             INTEGER,

  -- Rebound context
  rebound_type       TEXT,                     -- OFF, DEF, DEADBALL

  -- Foul context
  foul_type          TEXT,                     -- SHOOTING, OFFENSIVE, TECH
  free_throws_awarded INTEGER,

  -- Turnover context
  turnover_type      TEXT,                     -- BAD_PASS, TRAVEL, OUT_OF_BOUNDS

  -- Substitution context
  player_in_id       INTEGER,
  player_out_id      INTEGER,

  -- Score context
  home_score         INTEGER,
  away_score         INTEGER,
  score_margin       INTEGER,

  created_at         TIMESTAMP DEFAULT now(),

  PRIMARY KEY (game_id, event_num)
);

