CREATE TABLE player
(
    id BIGINT PRIMARY KEY,
    full_name VARCHAR(256),
    first_name VARCHAR(256),
    last_name VARCHAR(256),
    is_active BOOLEAN
);

CREATE TABLE historical_team_index
(
    id BIGINT,
    current_iteration BOOLEAN,
    city VARCHAR(256),
    nickname VARCHAR(256),
    year_founded INT,
    year_active_til INT,
    PRIMARY KEY(id, nickname, year_active_til)
);

CREATE TABLE modern_team_index
(
    id BIGINT, 
    abrev VARCHAR(3),
    nickname VARCHAR(256),
    PRIMARY KEY(id, abrev)
);

CREATE TABLE game
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

CREATE TABLE game_team_performance
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

CREATE TABLE pbp_raw_event(
    game_id BIGINT REFERENCES game(id),
    event_num INT NOT NULL,    
    event_type TEXT NOT NULL,  
    event_subtype TEXT,    
    home_score INT,
    away_score INT,
    period INT NOT NULL,
    clock INTERVAL NOT NULL, 
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
    FOREIGN KEY (event_team_id, event_team_abrev) REFERENCES modern_team_index (id, abrev),
    PRIMARY KEY (game_id, event_num)
);

/*
Enumeration for event_subtype: ['start', 'recovered', 'Jump Shot', 'defensive', 'bad pass', '',
       'DUNK', 'Layup', 'personal', 'offensive', 'full', '1 of 1', 'out',
       'in', 'offensive foul', 'out-of-bounds', '1 of 2', '2 of 2', 'end',
       'kicked ball', 'traveling', 'lost ball', 'Hook', 'technical',
       'offensive goaltending', 'defensive goaltending', 'illegal assist',
       'shot clock', 'delay-of-game', 'challenge', 'double dribble',
       '1 of 3', '2 of 3', '3 of 3', 'backcourt', 'inbound',
       'LaneViolation', 'other', 'punched ball', 'lane',
       'discontinued dribble', '3-second-violation',
       'offensive-kicked-ball', '5-second-violation',
       'illegal-out-of-bounds-screen', '8-second-violation',
       'jumpball violation']

Enumeration for event_type: ['period', 'jumpball', '2pt', 'rebound', '3pt', 'turnover', 'steal',
       'foul', 'timeout', 'block', 'freethrow', 'substitution',
       'violation', 'game', 'ejection']

Enumeration for decriptor: [nan, 'startperiod', 'pullup', 'floating', 'running', 'driving',
       'driving floating', 'fadeaway', 'tip', 'step back',
       'running pullup', 'loose ball', 'shooting', 'cutting',
       'turnaround', 'bad pass', 'putback', 'driving floating bank',
       'reverse', 'turnaround fadeaway', 'technical',
       'driving finger roll', 'step', 'lost ball', 'turnaround bank',
       'driving reverse', 'pullup bank', 'finger roll',
       'cutting finger roll', 'running finger roll', 'charge', 'bank',
       'flagrant-type-1', 'flagrant', 'driving bank', 'double',
       'alley-oop', 'fadeaway bank', 'off-the-ball', 'running alley-oop',
       'unclearpass', 'lodgedball', 'take', 'defensive-3-second',
       'running reverse', 'heldball', 'away-from-play', 'outofbounds',
       'transition take', 'doubleviolation', 'delay', 'flopping',
       'challenge', 'step back bank', 'flagrant-type-2',
       'turnaround fadeaway bank', 'rim-hanging']

Enumeration for area: [nan, 'Mid-Range', 'Above the Break 3', 'In The Paint (Non-RA)',
       'Restricted Area', 'Right Corner 3', 'Left Corner 3']

Enumeration for area_detail; [nan, '16-24 Center', '24+ Left Center', '0-8 Center',
       '16-24 Right Center', '24+ Center', '24+ Right',
       '16-24 Left Center', '8-16 Right', '24+ Right Center', '8-16 Left',
       '24+ Left', '16-24 Left', '8-16 Center', '16-24 Right']

Enumeration for side: [None, 'right', 'left']

'violation' eventType subtypes: ['kicked ball', 'defensive goaltending', 'delay-of-game', 'lane']
*/





