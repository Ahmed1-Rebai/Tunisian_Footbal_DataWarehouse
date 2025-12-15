"""
Définitions simplifiées du schéma (basées sur `sql/schema.sql`).
Fournit :
- listes de colonnes attendues (toutes)
- colonnes requises (minimum) pour valider les CSV générés

Remarque : les noms de colonnes sont normalisés en minuscules underscore.
"""

SCHEMA_DEFINITIONS = {
    "D_Stadium": {
        "all": ["id_stadium", "stadium_name", "capacity"],
        "required": ["stadium_name"]
    },
    "D_Team": {
        "all": ["id_team", "team_name", "id_city", "location"],
        "required": ["team_name"]
    },
    "D_Season": {
        "all": ["season_id", "season", "start_year", "end_year", "beforeafterindependence"],
        "required": ["season"]
    },
    "D_Competition": {
        "all": ["id_competition", "competition"],
        "required": ["competition"]
    },
    "D_Date": {
        "all": ["id_date", "date", "time", "year", "month", "day"],
        "required": ["date"]
    },
    "D_Player": {
        "all": ["id_player", "player_name", "birth_date", "nationality", "market_value"],
        "required": ["player_name"]
    },
    "D_Position": {
        "all": ["id_position", "position_name"],
        "required": ["position_name"]
    },
    "D_City": {
        "all": ["id_city", "city_name", "latitude", "longitude"],
        "required": ["city_name"]
    },
    "F_Champions": {
        "all": ["season_id", "competition_id", "winner_id", "runnerup_id", "score"],
        "required": ["season_id", "competition_id", "winner_id"]
    },
    "F_Team_Player_Season": {
        "all": ["season_id", "id_team", "id_player", "number", "id_position"],
        "required": ["season_id", "id_team", "id_player"]
    },
    "F_Match": {
        "all": ["id_match", "id_date", "id_home_team", "id_away_team", "id_competition", "season_id", "id_stadium", "result_home", "result_away", "penalties"],
        "required": ["id_home_team", "id_away_team", "id_competition", "season_id"]
    },
    "F_TopScorers_AllTime": {
        "all": ["id_player", "goals"],
        "required": ["id_player"]
    },
    "F_TopScorers_By_Season": {
        "all": ["season_id", "id_player", "goals"],
        "required": ["season_id", "id_player", "goals"]
    }
}

# Mappage par défaut nom de fichier -> table attendu
# Utilisez ce mapping si vos fichiers suivent la convention `D_*_clean.csv` ou `F_*.csv`
FILENAME_TO_TABLE = {
    "D_Team": ["d_team", "d_team_clean", "d_team_clean.csv"],
    "D_Competition": ["d_competition", "d_competition_clean"],
    "D_Season": ["d_season", "d_season_clean"],
    "D_Stadium": ["d_stadium", "d_stadium_clean"],
    "D_Date": ["d_date"],
    "F_Match": ["f_match", "f_match.csv"],
    "D_Champions": ["d_champions", "d_champions_clean"],
    "F_Champions": ["f_champions"],
    "D_Player": ["d_player", "d_player_clean"],
    "F_TopScorers_By_Season": ["d_topscorers_by_season","f_topscorers_by_season"],
    "F_TopScorers_AllTime": ["d_topscorers_alltime","f_topscorers_alltime"]
}
