-- ============================================================================
-- Data Warehouse Schema: Tunisian Football History - PostgreSQL Version
-- Complete schema with all dimensions, facts, and player rosters
-- ============================================================================

-- ============================================================================
-- DIMENSION TABLES (Base dimensions)
-- ============================================================================

CREATE TABLE D_Stadium (
  id_stadium SERIAL PRIMARY KEY,
  stadium_name VARCHAR(255) NOT NULL,
  capacity INT
);

CREATE TABLE D_City (
    id_city SERIAL PRIMARY KEY,
    city_name VARCHAR(255) UNIQUE NOT NULL,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6)
);

CREATE TABLE D_Team (
  id_team SERIAL PRIMARY KEY,
  team_name VARCHAR(255) NOT NULL,
  id_city INT,
  location VARCHAR(255),
  FOREIGN KEY (id_city) REFERENCES D_City(id_city)
);

CREATE TABLE D_Season (
  season_id SERIAL PRIMARY KEY,
  season VARCHAR(50) NOT NULL,
  start_year INT,
  end_year INT,
  BeforeAfterIndependence VARCHAR(50)
);

CREATE TABLE D_Competition (
  id_competition SERIAL PRIMARY KEY,
  competition VARCHAR(255) NOT NULL
);

CREATE TABLE D_Date (
  id_date SERIAL PRIMARY KEY,
  date DATE NOT NULL,
  time TIME,
  year INT,
  month INT,
  day INT
);

-- ============================================================================
-- PLAYER DIMENSION
-- ============================================================================

CREATE TABLE D_Player (
  id_player SERIAL PRIMARY KEY,
  player_name VARCHAR(255) NOT NULL,
  birth_date DATE,
  nationality VARCHAR(100)
);

CREATE TABLE D_Position (
    id_position SERIAL PRIMARY KEY,
    position_name VARCHAR(100) UNIQUE NOT NULL
);

-- ============================================================================
-- Fact TABLES: Historical / Reference Data
-- ============================================================================

CREATE TABLE F_Champions (
  season_id INT,
  competition_id INT,
  winner_id INT,
  runnerup_id INT,
  goal_winner INT,
  goal_runnerup INT,
  PRIMARY KEY (season_id, competition_id),
  FOREIGN KEY (season_id) REFERENCES D_Season(season_id),
  FOREIGN KEY (competition_id) REFERENCES D_Competition(id_competition),
  FOREIGN KEY (winner_id) REFERENCES D_Team(id_team),
  FOREIGN KEY (runnerup_id) REFERENCES D_Team(id_team)
);

CREATE TABLE F_Team_Player_Season (
    season_id INT NOT NULL,
    id_team INT NOT NULL,
    id_player INT NOT NULL,
    number VARCHAR(10),
    id_position INT NOT NULL,
    market_value INT,
    PRIMARY KEY (season_id, id_team, id_player),
    FOREIGN KEY (season_id) REFERENCES D_Season(season_id),
    FOREIGN KEY (id_team) REFERENCES D_Team(id_team),
    FOREIGN KEY (id_player) REFERENCES D_Player(id_player),
    FOREIGN KEY (id_position) REFERENCES D_Position(id_position)
);

-- ============================================================================
-- FACT TABLES
-- ============================================================================

CREATE TABLE F_Match (
  id_match SERIAL PRIMARY KEY,
  id_date INT,
  id_home_team INT,
  id_away_team INT,
  id_competition INT,
  season_id INT,
  id_stadium INT,
  result_home INT,
  result_away INT,
  penalties VARCHAR(50),
  FOREIGN KEY (id_home_team) REFERENCES D_Team(id_team),
  FOREIGN KEY (id_away_team) REFERENCES D_Team(id_team),
  FOREIGN KEY (id_competition) REFERENCES D_Competition(id_competition),
  FOREIGN KEY (season_id) REFERENCES D_Season(season_id),
  FOREIGN KEY (id_date) REFERENCES D_Date(id_date),
  FOREIGN KEY (id_stadium) REFERENCES D_Stadium(id_stadium)
);

-- Additional fact tables based on warehouse_output

CREATE TABLE D_Champions_All_time (
  -- Assuming structure similar to D_Champions
  season_id INT,
  competition_id INT,
  winner_id INT,
  runnerup_id INT,
  goal_winner INT,
  goal_runnerup INT,
  PRIMARY KEY (season_id, competition_id)
);

CREATE TABLE D_Champions (
  season_id INT,
  competition_id INT,
  winner_id INT,
  runnerup_id INT,
  goal_winner INT,
  goal_runnerup INT,
  PRIMARY KEY (season_id, competition_id)
);

CREATE TABLE D_Team_Seasons_clean (
  -- Assuming team season data
  season_id INT,
  id_team INT,
  -- Add other columns as needed
  PRIMARY KEY (season_id, id_team)
);

CREATE TABLE F_Team_Season (
  season_id INT,
  id_team INT,
  matches_total INT,
  matches_home INT,
  matches_away INT,
  wins INT,
  draws INT,
  losses INT,
  wins_home INT,
  wins_away INT,
  draws_home INT,
  draws_away INT,
  losses_home INT,
  losses_away INT,
  goals_for INT,
  goals_against INT,
  goals_diff INT,
  goals_for_home INT,
  goals_for_away INT,
  goals_against_home INT,
  goals_against_away INT,
  goals_per_match DECIMAL(5,2),
  goals_against_per_match DECIMAL(5,2),
  points INT,
  points_home INT,
  points_away INT,
  PRIMARY KEY (season_id, id_team)
);

CREATE TABLE F_TopScorers_AllTime (
  id_player INT,
  goals INT,
  -- Add other columns as needed
  PRIMARY KEY (id_player)
);

CREATE TABLE F_TopScorers_By_Season (
  season_id INT,
  id_player INT,
  goals INT,
  -- Add other columns as needed
  PRIMARY KEY (season_id, id_player)
);

-- ============================================================================
-- LOAD DATA FROM CSV FILES
-- ============================================================================

-- Note: Adjust paths if necessary. Assuming CSVs are in warehouse_output/ relative to the script execution.

COPY D_Stadium FROM 'warehouse_output/D_Stadium.csv' WITH CSV HEADER;
COPY D_City FROM 'warehouse_output/D_City.csv' WITH CSV HEADER;
COPY D_Team FROM 'warehouse_output/D_Team.csv' WITH CSV HEADER;
COPY D_Season FROM 'warehouse_output/D_Season.csv' WITH CSV HEADER;
COPY D_Competition FROM 'warehouse_output/D_Competition.csv' WITH CSV HEADER;
COPY D_Date FROM 'warehouse_output/D_Date.csv' WITH CSV HEADER;
COPY D_Player FROM 'warehouse_output/D_Player.csv' WITH CSV HEADER;
COPY D_Position FROM 'warehouse_output/D_Position.csv' WITH CSV HEADER; -- If exists
COPY D_Champions_All_time FROM 'warehouse_output/D_Champions_All_time.csv' WITH CSV HEADER;
COPY D_Champions FROM 'warehouse_output/D_Champions.csv' WITH CSV HEADER;
COPY D_Team_Seasons_clean FROM 'warehouse_output/D_Team_Seasons_clean.csv' WITH CSV HEADER;
COPY F_Champions FROM 'warehouse_output/F_Champions.csv' WITH CSV HEADER;
COPY F_Match FROM 'warehouse_output/F_Match.csv' WITH CSV HEADER;
COPY F_Team_Player_Season FROM 'warehouse_output/F_Team_Player_Season.csv' WITH CSV HEADER;
COPY F_Team_Season FROM 'warehouse_output/F_Team_Season.csv' WITH CSV HEADER;
COPY F_TopScorers_AllTime FROM 'warehouse_output/F_TopScorers_AllTime.csv' WITH CSV HEADER;
COPY F_TopScorers_By_Season FROM 'warehouse_output/F_TopScorers_By_Season.csv' WITH CSV HEADER;

-- ============================================================================
-- INDEXES
-- ============================================================================

CREATE INDEX IDX_Match_HomeTeam ON F_Match(id_home_team);
CREATE INDEX IDX_Match_AwayTeam ON F_Match(id_away_team);
CREATE INDEX IDX_Match_Season ON F_Match(season_id);
CREATE INDEX IDX_Match_Date ON F_Match(id_date);
CREATE INDEX IDX_Match_Competition ON F_Match(id_competition);

CREATE INDEX IDX_Champions_Season ON F_Champions(season_id);
CREATE INDEX IDX_TopScorers_Season ON F_TopScorers_By_Season(season_id);
CREATE INDEX IDX_TeamPlayer_Season ON F_Team_Player_Season(season_id, id_team);