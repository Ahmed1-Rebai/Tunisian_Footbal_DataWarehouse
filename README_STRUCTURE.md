# Project Structure - Tunisian Football Data Warehouse

## � Project Purpose

This project builds a comprehensive Data Warehouse for Tunisian football history, covering Ligue 1, Cup, and Super Cup competitions from 1907 to present. The goal is to create a centralized, analytical database that enables:

- **Historical Analysis**: Track team performances, player careers, and championship trends over time
- **Statistical Insights**: Generate complex reports on match outcomes, scoring patterns, and competitive dynamics
- **Data Preservation**: Maintain a reliable record of Tunisian football heritage for research and fan engagement
- **OLAP Analytics**: Support multi-dimensional analysis for sports analytics and business intelligence

The Galaxy Schema design ensures scalability and analytical flexibility for future expansions.

## �🏗️ ETL Architecture

The project uses a **Galaxy Schema** because it contains multiple fact tables sharing conformed dimensions. This approach:

- **Reduces redundancy** by reusing common dimensions
- **Enables complex analytics** across matches, seasons, and players
- **Optimizes OLAP queries** with shared referential integrity
- **Facilitates adding new fact tables** without duplication

See [Galaxy Schema](mermaid/galaxy_schema.md) for the complete model.

## Data Sources

- **Transfermarkt**: Main source for player market values, team rosters, and individual statistics
- **Flash Score**: Match results, live scores, and competition data
- **Wikipedia**: Complementary data for historical championships, team histories, and missing player information

All data is scraped, cleaned, and enriched through the ETL pipeline before loading into the galaxy schema.

```
tunisian_foot/
├── .gitignore
├── data/                      # Raw source data
│   ├── champions/             # Champions data
│   │   ├── cup_champions.csv
│   │   ├── ligue_1_champions.csv
│   │   └── super_cup_champions.csv
│   ├── matches/               # Match data
│   │   ├── cup/
│   │   ├── ligue_1/
│   │   └── super_cup/
│   └── player_data/           # Player data
│       ├── D_Season_clean.csv
│       ├── D_Team_clean.csv
│       └── tunisian_league_all_teams_seasons.csv
├── mermaid/                   # Mermaid diagrams
│   ├── galaxy_schema.md
│   └── project_overview.md
├── requirements.txt           # Python dependencies
├── schema.png                 # Visual schema
├── sql/                       # SQL scripts
│   ├── schema.sql             # DDL - Table schema
│   └── load_data_postgresql.sql  # PostgreSQL loading
├── src/                       # Source code
│   ├── config/                # Configuration
│   │   ├── database_config.py
│   │   └── schema_definitions.py
│   ├── etl.py                 # Main ETL script
│   └── tools/                 # Utility tools
│       ├── ensure_schema.py
│       └── validate_schema.py
├── theme/                     # Themes
│   └── theme.json
├── Dashboards/                # Data visualization dashboards
│   ├── 1.png                  # Dashboard screenshot 1
│   ├── 2.png                  # Dashboard screenshot 2
│   ├── 3.png                  # Dashboard screenshot 3
│   └── Demo.mp4               # Project demonstration video
└── warehouse_output/          # Cleaned ETL results
    ├── D_City.csv
    ├── D_Competition.csv
    ├── D_Date.csv
    ├── D_Player.csv
    ├── D_Position.csv
    ├── D_Season.csv
    ├── D_Stadium.csv
    ├── D_Team.csv
    ├── F_Champions.csv
    ├── F_Match.csv
    └── F_Team_Player_Season.csv
```

## 🔌 SQL Server Configuration

**Server:** `DESKTOP-MMF34HS\MSSQLSERVER01`

To configure the connection, edit `src/config/database_config.py`:
```python
SQL_SERVER_CONFIG = {
    'server': r'DESKTOP-MMF34HS\MSSQLSERVER01',
    'driver': '{ODBC Driver 17 for SQL Server}',
    'database': 'DW_Tunisia_Football',
    'trusted_connection': True,
}
```

## 📋 Processing Order

1. **Extraction** → Retrieves source data
2. **Transformation** → Cleans and enriches data
3. **Loading** → Loads data into SQL Server

## 🚀 Usage

```powershell
# Install dependencies
pip install pandas python-dateutil pyodbc

# Run main ETL
python src/etl.py

# Test SSMS connection
python src/config/database_config.py
```

## 📊 Dimensions & Facts

**Dimensions:**
- `D_City` - Cities
- `D_Competition` - Competitions
- `D_Date` - Dates
- `D_Player` - Players
- `D_Position` - Positions
- `D_Season` - Seasons
- `D_Stadium` - Stadiums
- `D_Team` - Teams

**Facts:**
- `F_Champions` - Champions by season/competition
- `F_Match` - Match results
- `F_Team_Player_Season` - Player/team/season statistics

## 🛠️ Scripts and Tools

### Main Script
- `src/etl.py` - Complete ETL pipeline

### Configuration
- `src/config/database_config.py` - Database configuration
- `src/config/schema_definitions.py` - Schema definitions

### Tools
- `src/tools/ensure_schema.py` - Schema verification
- `src/tools/validate_schema.py` - Schema validation

## 📝 Notes

- The `sql/schema.sql` script should **NEVER** be modified
- Source data is organized in `data/` with subfolders by type
- Cleaned results are generated in `warehouse_output/`
- Main code is in `src/`
