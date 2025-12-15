# Galaxy Schema (Constellation Schema) - Tunisian Football DW

```mermaid
graph TD
    %% Shared Dimensions
    D1[D_City<br/>id_city, city_name, latitude, longitude]
    D2[D_Competition<br/>id_competition, competition]
    D3[D_Season<br/>season_id, season, start_year, end_year]
    D4[D_Stadium<br/>id_stadium, stadium_name, capacity]
    D5[D_Date<br/>id_date, date, year, month, day]
    D6[D_Player<br/>id_player, player_name, birth_date, nationality]
    D7[D_Position<br/>id_position, position_name]
    D8[D_Team<br/>id_team, team_name, id_city, location]

    %% Fact Tables
    F1[F_Match<br/>id_match, id_date, id_home_team,<br/>id_away_team, id_competition,<br/>season_id, id_stadium,<br/>result_home, result_away]
    F2[F_Team_Player_Season<br/>season_id, id_team, id_player,<br/>number, id_position, market_value]
    F3[F_Champions<br/>season_id, competition_id,<br/>winner_id, runnerup_id,<br/>goal_winner, goal_runnerup]

    %% Connections
    D8 --> D1  %% D_Team references D_City

    F1 --> D5
    F1 --> D8  %% home_team and away_team
    F1 --> D2
    F1 --> D3
    F1 --> D4

    F2 --> D3
    F2 --> D8
    F2 --> D6
    F2 --> D7

    F3 --> D3
    F3 --> D2
    F3 --> D8  %% winner_id and runnerup_id

    %% Styling
    classDef dimension fill:#dae8fc,stroke:#6c8ebf
    classDef fact fill:#ffe6cc,stroke:#d79b00

    class D1,D2,D3,D4,D5,D6,D7,D8 dimension
    class F1,F2,F3 fact
```

## Why Galaxy Schema?

The **Galaxy Schema** (also called Constellation Schema) is used here because:

- **Multiple Fact Tables**: Our DW has several fact tables (F_Match, F_Team_Player_Season, F_Champions) that share common dimensions.
- **Shared Dimensions**: Dimensions like D_Team, D_Season, D_Player are reused across multiple facts, reducing redundancy.
- **Complex Analytics**: Allows complex queries across different business areas (matches, seasons, players) while maintaining referential integrity.
- **Scalability**: Easier to add new fact tables without duplicating dimensions.
- **Performance**: Optimized for OLAP queries with shared conformed dimensions.

## Data Sources

- **Primary Sources**: Data scraped from **Transfermarkt** (player values, team info) and **Flash Score** (match results, statistics).
- **Complementary**: Missing data supplemented from **Wikipedia** (historical championships, team histories, player biographies).
- **ETL Process**: Raw scraped data is cleaned, transformed, and loaded into the galaxy schema for analysis.
