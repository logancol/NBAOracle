CREATE TABLE player
(
    id INT PRIMARY KEY,
    full_name VARCHAR(256),
    first_name VARCHAR(256),
    last_name VARCHAR(256),
    is_active BOOLEAN
);

CREATE TABLE historical_team_index
(
    id INT,
    current_iteration BOOLEAN,
    city VARCHAR(256),
    nickname VARCHAR(256),
    year_founded INT,
    year_active_til INT,
    PRIMARY KEY(id, nickname, year_active_til)
);

CREATE TABLE modern_team_index
(
    id INT, 
    abrev VARCHAR(256),
    nickname VARCHAR(256),
    PRIMARY KEY(id, abrev)
);

CREATE TABLE game
(
    id INT PRIMARY KEY,
    season_id INT,
    home_team_id INT,
    home_team_abrev VARCHAR(256),
    away_team_id INT,
    away_team_abrev VARCHAR(256),
    date DATE,
    season_type TEXT,
    winner_id INT,
    FOREIGN KEY (home_team_id, home_team_abrev) REFERENCES modern_team_index (id, abrev),
    FOREIGN KEY (away_team_id, away_team_abrev) REFERENCES modern_team_index (id, abrev)
);

CREATE TABLE game_team_performance
(
    game_id INT REFERENCES game(id),
    team_id INT,
    team_abrev VARCHAR(256),
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

CREATE TABLE pbp_raw_event(
    game_id INT REFERENCES game(id),
    event_num INT NOT NULL,    
    event_type TEXT NOT NULL,  
    event_subtype TEXT,    
    home_score INTEGER,
    away_score INTEGER,
    period INTEGER NOT NULL,
    clock INTERVAL NOT NULL, 
    home_team_id INTEGER REFERENCES Team(team_id),
    away_team_id INTEGER REFERENCES Team(team_id),
    possession_team_id INTEGER REFERENCES Team(team_id),
    is_overtime BOOLEAN,
    
    shooter_id INT REFERENCES Player(id),
    assister_id INT REFERENCES Player(id),
    jump_ball_winner_id INT REFERENCES Player(id),
    jump_ball_loser_id INT REFERENCES Player(id),
    jump_ball_recovered_id INT REFERENCES Player(id),
    rebounder_id INT REFERENCES Player(id),
    foul_drawn_id INT REFERENCES Player(id),
    fouler_id INT REFERENCES Player(id),
    steal_id INT REFERENCES Player(id),
    block_id INT REFERENCES Player(id),
    sub_in_id INT REFERENCES Player(id),
    sub_out_id INT REFERENCES Player(id),

    foul_is_technical BOOLEAN,
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

    created_at         TIMESTAMP DEFAULT now(),

    PRIMARY KEY (game_id, event_num)
);

