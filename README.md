# Tunisian Football Data Warehouse

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-1.3+-orange.svg)](https://pandas.pydata.org/)
[![SQL Server](https://img.shields.io/badge/SQL_Server-2019+-red.svg)](https://www.microsoft.com/en-us/sql-server)

Un pipeline ETL complet pour construire un Data Warehouse de l'histoire du football tunisien, incluant la Ligue 1, la Coupe et la Super Coupe.

## 📋 Vue d'ensemble

Ce projet extrait, transforme et charge des données historiques de football depuis des sources CSV dans un Data Warehouse SQL Server structuré. Il couvre les résultats de matchs, statistiques d'équipes, données de joueurs et championnats de la ligue tunisienne de football.

## 🏗️ Architecture

Le projet suit une architecture ETL standard :

- **Extraction** : Analyse et normalisation des fichiers CSV depuis diverses sources
- **Transformation** : Nettoyage, validation et enrichissement des données
- **Chargement** : Insertion des données traitées dans les tables SQL Server

### Modèle de données : Schéma en constellation (Galaxy Schema)

Ce Data Warehouse utilise un **schéma en constellation** car il contient plusieurs tables de faits partageant des dimensions conformées. Cette conception :

- **Réduit la redondance** en réutilisant les dimensions communes
- **Permet des analyses complexes** sur les matchs, saisons et joueurs
- **Optimise les requêtes OLAP** avec intégrité référentielle partagée
- **Facilite l'ajout de nouvelles tables de faits** sans duplication

**Dimensions :**
- `D_Champions_All_time` - Champions de tous les temps
- `D_Champions` - Champions par saison
- `D_Competition` - Compétitions
- `D_Date` - Dates
- `D_Player` - Joueurs
- `D_Season` - Saisons
- `D_Stadium` - Stades
- `D_Team` - Équipes
- `D_Team_Seasons_clean` - Saisons des équipes nettoyées

**Tables de faits :**
- `F_Champions` - Champions par saison/compétition
- `F_Match` - Résultats des matchs
- `F_Team_Player_Season` - Statistiques joueur/équipe/saison
- `F_Team_Season` - Statistiques équipe/saison
- `F_TopScorers_AllTime` - Meilleurs buteurs de tous les temps
- `F_TopScorers_By_Season` - Meilleurs buteurs par saison

Voir [Schéma Galaxy](mermaid/galaxy_schema.md) pour le modèle complet.

## 🚀 Démarrage rapide

### Prérequis

- Python 3.8+
- SQL Server (local ou distant)
- ODBC Driver 17 for SQL Server

### Installation

```bash
# Cloner le dépôt
git clone https://github.com/yourusername/tunisian-football-dw.git
cd tunisian-football-dw

# Installer les dépendances
pip install -r requirements.txt
```

### Utilisation

1. **Configurer la base** : Exécuter `sql/schema.sql` pour créer les tables
2. **Configurer la connexion** : Mettre à jour `config/database_config.py` avec vos détails SQL Server
3. **Lancer l'ETL** : Exécuter `python etl_tunisia_dw.py`

## 📁 Structure du projet

```
tunisian-football-dw/
├── etl_tunisia_dw.py          # Orchestrateur ETL principal
├── etl/
│   ├── extraction/            # Scripts d'extraction des données
│   │   ├── extract_match.py
│   │   ├── inspect_csv.py
│   │   └── review_extraction.py
│   ├── transformation/        # Nettoyage et transformation
│   │   ├── add_time_column.py
│   │   ├── check_duplicates.py
│   │   ├── check_nulls.py
│   │   ├── clean_date_dimension.py
│   │   ├── enrich_match_stadium.py
│   │   ├── fix_*.py            # Scripts de correction
│   │   ├── generate_team_season_agg.py
│   │   ├── rebuild_champions_*.py
│   │   └── produce_minimal_team_seasons.py
│   └── loading/               # Chargement en base
│       ├── load_*.py          # Scripts de chargement
│       ├── reload_*.py        # Scripts de rechargement
│       └── fill_*.py          # Scripts de remplissage
├── config/
│   └── database_config.py     # Configuration base de données
├── sql/
│   ├── schema.sql             # DDL du schéma
│   └── load_data_postgresql.sql  # Chargement PostgreSQL
├── data/                      # Données sources brutes
│   ├── D_*.csv                # Données sources
│   ├── champions/
│   ├── matches/
│   └── player_data/
├── warehouse_output/          # Sorties ETL nettoyées
│   ├── D_*.csv                # Dimensions nettoyées
│   ├── F_*.csv                # Tables de faits
│   └── *_mappings.csv         # Mappings et audits
├── mermaid/                   # Diagrammes Mermaid
│   ├── galaxy_schema.md
│   └── project_overview.md
├── temp/                      # Fichiers temporaires
├── tools/                     # Outils utilitaires
├── theme/                     # Thèmes
└── documentation/             # Documentation
    └── modeling.drawio
```

## 📊 Pipeline ETL

Voir [Diagramme du pipeline ETL](mermaid/project_overview.md) pour l'architecture visuelle.

## � Dashboards & Visualization

The project includes interactive dashboards for data visualization and analysis:

- **Dashboard Screenshots**: View sample visualizations in `Dashboards/` folder
- **Demo Video**: Watch the project demonstration in `Dashboards/Demo.mp4`
- **Key Visualizations**:
  - Team performance analytics over seasons
  - Player statistics and market value trends
  - Championship winners and competition analysis
  - Match results and scoring patterns

## �📖 Documentation

- [Détails du processus ETL](README_ETL.md)
- [Guide de structure du projet](README_STRUCTURE.md)
- [Schéma de base de données](sql/schema.sql)

## 🤝 Contribution

1. Forker le dépôt
2. Créer une branche de fonctionnalité
3. Faire vos modifications
4. Soumettre une pull request

## 📄 Licence

Ce projet est sous licence MIT - voir le fichier LICENSE pour les détails.

## 📞 Contact

Pour des questions ou problèmes, ouvrez une issue GitHub.