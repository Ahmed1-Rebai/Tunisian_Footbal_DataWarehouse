-- ============================================================================
-- Data Warehouse Schema: Tunisian Football History
-- Complete schema with all dimensions, facts, and player rosters
-- ============================================================================

-- ============================================================================
-- DIMENSION TABLES (Base dimensions)
-- ============================================================================

CREATE TABLE D_Stadium (
  id_stadium INT IDENTITY(1,1) PRIMARY KEY,
  stadium_name VARCHAR(255) NOT NULL,
  capacity INT
);

CREATE TABLE D_City (
    id_city INT IDENTITY(1,1) PRIMARY KEY,
    city_name VARCHAR(255) UNIQUE NOT NULL,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6)
);

CREATE TABLE D_Team (
  id_team INT IDENTITY(1,1) PRIMARY KEY,
  team_name VARCHAR(255) NOT NULL,
  id_city INT,
  location VARCHAR(255),
  FOREIGN KEY (id_city) REFERENCES D_City(id_city)
);

CREATE TABLE D_Season (
  season_id INT IDENTITY(1,1) PRIMARY KEY,
  season VARCHAR(50) NOT NULL,
  start_year INT,
  end_year INT,
  BeforeAfterIndependence VARCHAR(50)
);

CREATE TABLE D_Competition (
  id_competition INT IDENTITY(1,1) PRIMARY KEY,
  competition VARCHAR(255) NOT NULL
);

CREATE TABLE D_Date (
  id_date INT IDENTITY(1,1) PRIMARY KEY,
  date DATE NOT NULL,
  time TIME,
  year INT,
  month INT,
  day INT
);

-- ============================================================================
-- PLAYER DIMENSION
-- ============================================================================

-- D_Player: Player master dimension
CREATE TABLE D_Player (
  id_player INT IDENTITY(1,1) PRIMARY KEY,
  player_name VARCHAR(255) NOT NULL,
  birth_date DATE,
  nationality VARCHAR(100),
);

-- D_Position: Player positions reference table
CREATE TABLE D_Position (
    id_position INT IDENTITY(1,1) PRIMARY KEY,
    position_name VARCHAR(100) UNIQUE NOT NULL
);


-- ============================================================================
-- Fact TABLES: Historical / Reference Data
-- ============================================================================

-- F_Champions: Championship history (League, Cup, Super Cup)
CREATE TABLE F_Champions (
  season_id INT ,
  competition_id INT,
  winner_id INT,
  runnerup_id INT,
  goal_winner int,
  goal_runnerup int,

  PRIMARY KEY (season_id, competition_id),

  FOREIGN KEY (season_id) REFERENCES D_Season(season_id),
  FOREIGN KEY (competition_id) REFERENCES D_Competition(id_competition),
  FOREIGN KEY (winner_id) REFERENCES D_Team(id_team),
  FOREIGN KEY (runnerup_id) REFERENCES D_Team(id_team)
);




-- D_Team_Player_Season: Team player assignments per season (normalized fact table)
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

-- F_Match: Match-level details (detailed results, scores, venues)
CREATE TABLE F_Match (
  id_match INT IDENTITY(1,1) PRIMARY KEY,
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




-- Contraintes UNIQUE pour éviter les doublons
ALTER TABLE D_Team 
ADD CONSTRAINT UQ_Team_Name UNIQUE(team_name);

ALTER TABLE D_Competition 
ADD CONSTRAINT UQ_Competition UNIQUE(competition);

ALTER TABLE D_Stadium 
ADD CONSTRAINT UQ_Stadium_Name UNIQUE(stadium_name);

-- Index sur F_Match (table la plus utilisée)
CREATE INDEX IDX_Match_HomeTeam ON F_Match(id_home_team);
CREATE INDEX IDX_Match_AwayTeam ON F_Match(id_away_team);
CREATE INDEX IDX_Match_Season ON F_Match(season_id);
CREATE INDEX IDX_Match_Date ON F_Match(id_date);
CREATE INDEX IDX_Match_Competition ON F_Match(id_competition);

-- Index sur les tables de faits
CREATE INDEX IDX_Champions_Season ON F_Champions(season_id);
CREATE INDEX IDX_TopScorers_Season ON F_TopScorers_By_Season(season_id);
CREATE INDEX IDX_TeamPlayer_Season ON F_Team_Player_Season(season_id, id_team);

CREATE OR ALTER VIEW V_Team_Season AS
WITH HomeStats AS (
    SELECT
        season_id,
        id_home_team AS id_team,&
        COUNT(*) AS matches_home,
        SUM(CASE WHEN result_home > result_away THEN 1 ELSE 0 END) AS wins_home,
        SUM(CASE WHEN result_home = result_away THEN 1 ELSE 0 END) AS draws_home,
        SUM(CASE WHEN result_home < result_away THEN 1 ELSE 0 END) AS losses_home,
        SUM(result_home) AS goals_for_home,
        SUM(result_away) AS goals_against_home
    FROM F_Match
    GROUP BY season_id, id_home_team
),
AwayStats AS (
    SELECT
        season_id,
        id_away_team AS id_team,
        COUNT(*) AS matches_away,
        SUM(CASE WHEN result_away > result_home THEN 1 ELSE 0 END) AS wins_away,
        SUM(CASE WHEN result_away = result_home THEN 1 ELSE 0 END) AS draws_away,
        SUM(CASE WHEN result_away < result_home THEN 1 ELSE 0 END) AS losses_away,
        SUM(result_away) AS goals_for_away,
        SUM(result_home) AS goals_against_away
    FROM F_Match
    GROUP BY season_id, id_away_team
)
SELECT
    COALESCE(h.season_id, a.season_id) AS season_id,
    COALESCE(h.id_team, a.id_team) AS id_team,
    COALESCE(matches_home,0) + COALESCE(matches_away,0) AS matches_total,
    COALESCE(matches_home,0) AS matches_home,
    COALESCE(matches_away,0) AS matches_away,
    COALESCE(wins_home,0) + COALESCE(wins_away,0) AS wins,
    COALESCE(draws_home,0) + COALESCE(draws_away,0) AS draws,
    COALESCE(losses_home,0) + COALESCE(losses_away,0) AS losses,
    COALESCE(wins_home,0) AS wins_home,
    COALESCE(wins_away,0) AS wins_away,
    COALESCE(draws_home,0) AS draws_home,
    COALESCE(draws_away,0) AS draws_away,
    COALESCE(losses_home,0) AS losses_home,
    COALESCE(losses_away,0) AS losses_away,
    COALESCE(goals_for_home,0) + COALESCE(goals_for_away,0) AS goals_for,
    COALESCE(goals_against_home,0) + COALESCE(goals_against_away,0) AS goals_against,
    (COALESCE(goals_for_home,0) + COALESCE(goals_for_away,0)) - (COALESCE(goals_against_home,0) + COALESCE(goals_against_away,0)) AS goals_diff,
    COALESCE(goals_for_home,0) AS goals_for_home,
    COALESCE(goals_for_away,0) AS goals_for_away,
    COALESCE(goals_against_home,0) AS goals_against_home,
    COALESCE(goals_against_away,0) AS goals_against_away,
    -- Moyennes
    CAST((COALESCE(goals_for_home,0) + COALESCE(goals_for_away,0)) * 1.0 / NULLIF((COALESCE(matches_home,0)+COALESCE(matches_away,0)),0) AS DECIMAL(5,2)) AS goals_per_match,
    CAST((COALESCE(goals_against_home,0) + COALESCE(goals_against_away,0)) * 1.0 / NULLIF((COALESCE(matches_home,0)+COALESCE(matches_away,0)),0) AS DECIMAL(5,2)) AS goals_against_per_match,
    -- Points pour classement
    (COALESCE(wins_home,0) + COALESCE(wins_away,0)) * 3 + (COALESCE(draws_home,0) + COALESCE(draws_away,0)) AS points,
    COALESCE(wins_home,0)*3 + COALESCE(draws_home,0) AS points_home,
    COALESCE(wins_away,0)*3 + COALESCE(draws_away,0) AS points_away
FROM HomeStats h
FULL OUTER JOIN AwayStats a
ON h.season_id = a.season_id AND h.id_team = a.id_team;

