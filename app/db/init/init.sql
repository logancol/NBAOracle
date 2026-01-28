CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(256),
    password_hash VARCHAR(256) NOT NULL,
    email CITEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS player
(
    id BIGINT PRIMARY KEY,
    full_name VARCHAR(256),
    first_name VARCHAR(256),
    last_name VARCHAR(256),
    is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS historical_team_index
(
    id BIGINT,
    current_iteration BOOLEAN,
    city VARCHAR(256),
    nickname VARCHAR(256),
    year_founded INT,
    year_active_til INT,
    PRIMARY KEY(id, nickname, year_active_til)
);

CREATE TABLE IF NOT EXISTS modern_team_index
(
    id BIGINT, 
    abrev VARCHAR(3),
    nickname VARCHAR(256),
    PRIMARY KEY(id, abrev)
);

CREATE TABLE IF NOT EXISTS game
(
    id BIGINT PRIMARY KEY,
    season_id INT,
    home_team_id BIGINT,
    home_team_abrev VARCHAR(3),
    away_team_id BIGINT,
    away_team_abrev VARCHAR(3),
    date DATE,
    season_type TEXT,
    winner_id BIGINT,
    FOREIGN KEY (home_team_id, home_team_abrev) REFERENCES modern_team_index (id, abrev),
    FOREIGN KEY (away_team_id, away_team_abrev) REFERENCES modern_team_index (id, abrev)
);

CREATE TABLE IF NOT EXISTS game_team_performance
(
    game_id BIGINT REFERENCES game(id),
    team_id BIGINT,
    team_abrev VARCHAR(3),
    mins INT,
    pts INT,
    overtime BOOLEAN,
    field_goals_made INT,
    field_goals_attempted INT,
    field_goal_percentage FLOAT,
    three_pointers_made INT,
    three_pointers_attempted INT,
    three_pointer_percentage FLOAT,
    free_throws_made INT,
    free_throws_attempted INT,
    free_throw_percentage FLOAT,
    offensive_rebounds INT,
    defensive_rebounds INT, 
    total_rebounds INT,
    assists INT,
    steals INT,
    blocks INT,
    turnovers INT,
    personal_fouls INT,
    plus_minus INT,
    PRIMARY KEY (game_id, team_id),
    FOREIGN KEY (team_id, team_abrev) REFERENCES modern_team_index (id, abrev)
);

CREATE TABLE IF NOT EXISTS pbp_raw_event(
    id SERIAL NOT NULL PRIMARY KEY,
    game_id BIGINT REFERENCES game(id),
    season_id INT,
    season_type TEXT,
    event_num INT NOT NULL,    
    event_type TEXT NOT NULL,  
    event_subtype TEXT,    
    home_score INT,
    away_score INT,
    period INT NOT NULL,
    clock INTERVAL, 
    home_team_id BIGINT,
    away_team_id BIGINT,
    home_team_abrev VARCHAR(3),
    away_team_abrev VARCHAR(3),
    possession_team_id BIGINT,
    possession_team_abrev VARCHAR(3),
    event_team_id BIGINT,
    event_team_abrev VARCHAR(3),
    is_overtime BOOLEAN,
    
    -- Actor IDs
    shooter_id BIGINT REFERENCES Player(id) NULL,
    assister_id BIGINT REFERENCES Player(id) NULL,
    jump_ball_winner_id BIGINT REFERENCES Player(id) NULL,
    jump_ball_loser_id BIGINT REFERENCES Player(id) NULL,
    jump_ball_recovered_id BIGINT REFERENCES Player(id) NULL,
    rebounder_id BIGINT REFERENCES Player(id) NULL,
    turnover_id BIGINT REFERENCES Player(id) NULL,
    foul_drawn_id BIGINT REFERENCES Player(id) NULL,
    fouler_id BIGINT REFERENCES Player(id) NULL,
    stealer_id BIGINT REFERENCES Player(id) NULL,
    blocker_id BIGINT REFERENCES Player(id) NULL,
    sub_in_id BIGINT REFERENCES Player(id) NULL,
    sub_out_id BIGINT REFERENCES Player(id) NULL,

    foul_is_technical BOOLEAN,
    foul_is_personal BOOLEAN,
    foul_is_offensive BOOLEAN,
    team_turnover BOOLEAN,
    team_rebound BOOLEAN,
    offensive_rebound BOOLEAN,
    side TEXT,
    descriptor TEXT,
    area TEXT,
    area_detail TEXT,
    shot_distance FLOAT,
    shot_made BOOLEAN,
    shot_value INT,
    shot_x FLOAT,                  
    shot_y FLOAT,

    created_at TIMESTAMP DEFAULT now(),
    FOREIGN KEY (home_team_id, home_team_abrev) REFERENCES modern_team_index (id, abrev),
    FOREIGN KEY (away_team_id, away_team_abrev) REFERENCES modern_team_index (id, abrev),
    FOREIGN KEY (possession_team_id, possession_team_abrev) REFERENCES modern_team_index (id, abrev),
    FOREIGN KEY (event_team_id, event_team_abrev) REFERENCES modern_team_index (id, abrev)
);

-- Creating new tables to help the model in weird scenarios
CREATE TABLE IF NOT EXISTS possessions(
    game_id BIGINT REFERENCES game(id),
    possession_id SERIAL PRIMARY KEY,
    offense_team_id BIGINT,
    defense_team_id BIGINT,
    start_pbp_event INT REFERENCES pbp_raw_event(id),
    end_pbp_event INT REFERENCES pbp_raw_event(id),
    possession_period INT,
    start_clock INTERVAL,
    end_clock INTERVAL,
    points INT,
    result VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS player_stints(
    id SERIAL PRIMARY KEY,
    game_id BIGINT REFERENCES game(id) NOT NULL,
    player_id BIGINT REFERENCES player(id) NOT NULL,
    team_id BIGINT,
    start_pbp_event INT REFERENCES pbp_raw_event(id),
    end_pbp_event INT REFERENCES pbp_raw_event(id),
    start_clock INTERVAL,
    end_clock INTERVAL,
    possession_period INT,
    seconds_played INT
)

CREATE TABLE IF NOT EXISTS shots(
    game_id BIGINT REFERENCES game(id) NOT NULL,
    shooter_id BIGINT REFERENCES player(id) NOT NULL,
    team_id BIGINT,
    shot_x FLOAT,
    shot_y FLOAT,
    zone
    ....
)