"""
ETL minimal pour construire un Data Warehouse historique (Tunisie: Ligue 1, Cup, Super Cup)
- parcours `data/matches/**.csv` pour construire `F_Match`
- construit dimensions: `D_Team`, `D_Competition`, `D_Season`, `D_Stadium`, `D_Date`
- produit CSV nettoyés dans `warehouse_output/`

Usage (PowerShell):
> python -m pip install pandas python-dateutil
> python src/etl.py

Le script est conservateur (heuristiques pour noms de colonnes) — adaptez si besoin.
"""

import os
import glob
import pandas as pd
from dateutil import parser
from pathlib import Path
import pyodbc

# Database connection
connection_string = """
Driver={ODBC Driver 17 for SQL Server};
Server=DESKTOP-MMF34HS\\MSSQLSERVER01;
Database=tunisian_foot;
Trusted_Connection=yes;
"""

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / 'data'
MATCHES_GLOB = str(DATA_DIR / 'matches' / '**' / '*.csv')
OUTPUT_DIR = ROOT / 'warehouse_output'
OUTPUT_DIR.mkdir(exist_ok=True)

# Helpers pour normaliser noms de colonnes
def normalize_cols(df):
    df = df.copy()
    df.columns = [c.strip().lower().replace(' ', '_').replace('.', '_') for c in df.columns]
    return df

# Heuristiques pour trouver colonnes importantes
def pick_col(cols, candidates):
    for cand in candidates:
        for c in cols:
            if cand == c:
                return c
    # fuzzy contains
    for cand in candidates:
        for c in cols:
            if cand in c:
                return c
    return None

# lire et normaliser un fichier de matches
def read_match_file(path):
    try:
        df = pd.read_csv(path)
    except Exception:
        df = pd.read_csv(path, encoding='latin1')
    df = normalize_cols(df)
    # détecter colonnes
    cols = df.columns.tolist()
    id_col = pick_col(cols, ['matchid','id_match','id','match_id'])
    stage_col = pick_col(cols, ['stage','round','phase'])
    status_col = pick_col(cols, ['status','state'])
    date_col = pick_col(cols, ['date','match_date','kickoff'])
    home_col = pick_col(cols, ['home_name','home.name','home','home_team','home.name'])
    away_col = pick_col(cols, ['away_name','away.name','away','away_team','away.name'])
    res_home = pick_col(cols, ['result_home','result.home','home_score','result.home'])
    res_away = pick_col(cols, ['result_away','result.away','away_score','result.away'])
    reg_col = pick_col(cols, ['result_regulationtime','result_regulation_time','regulation_time','result_regulationtime','result_regulation'])
    pen_col = pick_col(cols, ['result_penalties','result.penalties','penalties'])
    venue_col = pick_col(cols, ['information_venue','information.venue','venue','stadium','stadium_name'])
    cap_col = pick_col(cols, ['information_capacity','information.capacity','capacity','stadium_capacity'])

    # extraire colonnes fiables
    out = pd.DataFrame()
    out['id_match'] = df[id_col] if id_col in df.columns else df.index.astype(str)
    out['stage'] = df[stage_col] if stage_col in df.columns else None
    out['status'] = df[status_col] if status_col in df.columns else None
    out['date_raw'] = df[date_col] if date_col in df.columns else None
    out['home_team_name'] = df[home_col] if home_col in df.columns else None
    out['away_team_name'] = df[away_col] if away_col in df.columns else None
    out['result_home'] = df[res_home] if res_home in df.columns else None
    out['result_away'] = df[res_away] if res_away in df.columns else None
    out['regulation_time'] = df[reg_col] if reg_col in df.columns else None
    out['penalties'] = df[pen_col] if pen_col in df.columns else None
    out['venue'] = df[venue_col] if venue_col in df.columns else None
    out['capacity'] = df[cap_col] if cap_col in df.columns else None

    # competition / season deduced depuis le chemin et le nom de fichier
    p = str(path).replace('\\','/')
    # example: .../matches/ligue_1/tunisia_ligue_professionnelle_1_2019_2020.csv
    #          .../matches/cup/tunisia_tunisia_cup_tunisia_cup_2010_2011.csv
    #          .../matches/super_cup/super_cup_super_cup_2019.csv
    parts = p.split('/')
    comp = None
    season = None
    # si dossier parent contient ligue_1, cup, super_cup, on le prend
    for part in parts:
        low = part.lower()
        if low in ('ligue_1','cup','super_cup','supercup'):
            comp = low
    
    # try parse season from filename (robust logic for all patterns)
    fname = os.path.basename(path)
    import re
    
    # Pattern 1: yyyy_yyyy or yyyy-yyyy (e.g. 2019_2020, 2000-01, 2010_2011)
    m = re.search(r'(\d{4})[\-_](\d{2,4})', fname)
    if m:
        year1 = m.group(1)
        year2 = m.group(2)
        # Normalize to 2-digit year if needed
        if len(year2) == 4:
            season = f"{year1}-{year2[2:]}"  # 2019_2020 -> 2019-20
        else:
            season = f"{year1}-{year2}"  # 2000-01 or 2010-11
    else:
        # Pattern 2: Single year at end (e.g. super_cup_2019) -> assume 2019-20
        m2 = re.search(r'(\d{4})(?:\.csv)?$', fname)
        if m2:
            year = int(m2.group(1))
            # For single year, assume it refers to season year-to-year+1
            # But check if it's a super_cup or cup (special handling)
            if comp in ('super_cup', 'cup'):
                # super_cup_2019 -> 2019-20
                season = f"{year}-{str(year+1)[2:]}"
            else:
                # For ligue_1 with single year, might be incomplete data
                season = str(year)
    
    out['competition'] = comp
    out['season'] = season
    out['source_file'] = path
    return out


def parse_date_safe(s):
    if pd.isna(s):
        return None
    try:
        return parser.parse(str(s), dayfirst=True)
    except Exception:
        try:
            return pd.to_datetime(s, errors='coerce')
        except Exception:
            return None


def build_dimensions(matches_df, data_dir):
    # D_Team: use existing D_Team.csv if present to keep ids
    teams_file = data_dir / 'D_Team.csv'
    if teams_file.exists():
        dteam = pd.read_csv(teams_file)
        # robust mapping: detect name and id columns and preserve id if present
        lower_map = {c.lower(): c for c in dteam.columns}
        name_col_l = pick_col(list(lower_map.keys()), ['team', 'team_name', 'name'])
        id_col_l = pick_col(list(lower_map.keys()), ['id_team', 'team_id', 'id'])
        location_col_l = pick_col(list(lower_map.keys()), ['location', 'city', 'ville'])
        stadium_id_col_l = pick_col(list(lower_map.keys()), ['stadium_id', 'stade_id', 'stadium'])
        
        if name_col_l and name_col_l in lower_map:
            name_orig = lower_map[name_col_l]
        else:
            name_orig = dteam.columns[0]

        if id_col_l and id_col_l in lower_map:
            id_orig = lower_map[id_col_l]
            # if name_orig accidentally matched the id column (file order), pick another column for name
            if name_orig == id_orig:
                # choose first non-id column as team name
                name_candidates = [c for c in dteam.columns if c != id_orig]
                if len(name_candidates) > 0:
                    name_orig = name_candidates[0]
            # keep provided ids
            renames = {name_orig: 'team_name', id_orig: 'id_team'}
            if location_col_l and location_col_l in lower_map:
                renames[lower_map[location_col_l]] = 'location'
            if stadium_id_col_l and stadium_id_col_l in lower_map:
                renames[lower_map[stadium_id_col_l]] = 'stadium_id'
            
            dteam = dteam.rename(columns=renames)
            # if there is no team_name column after rename, fall back to creating from remaining cols
            if 'team_name' not in dteam.columns:
                # attempt to find likely name column
                for cand in ['nom_equipe', 'equipe', 'club', 'team_name', 'name']:
                    if cand in dteam.columns:
                        dteam = dteam.rename(columns={cand: 'team_name'})
                        break
            
            # Select only relevant columns
            cols_to_keep = ['team_name', 'id_team']
            if 'location' in dteam.columns:
                cols_to_keep.append('location')
            if 'stadium_id' in dteam.columns:
                cols_to_keep.append('stadium_id')
            
            dteam = dteam[cols_to_keep].drop_duplicates().reset_index(drop=True)
            
            # Ensure location and stadium_id columns exist
            if 'location' not in dteam.columns:
                dteam['location'] = None
            if 'stadium_id' not in dteam.columns:
                dteam['stadium_id'] = None
        else:
            # no id provided: generate sequential ids
            dteam = dteam.rename(columns={name_orig: 'team_name'})
            dteam = dteam[['team_name']].drop_duplicates().reset_index(drop=True)
            dteam['id_team'] = range(1, len(dteam) + 1)
            dteam['location'] = None
            dteam['stadium_id'] = None
    else:
        teams = pd.concat([matches_df['home_team_name'], matches_df['away_team_name']]).dropna().unique()
        dteam = pd.DataFrame({'team_name': sorted(teams)})
        dteam['id_team'] = range(1, len(dteam)+1)
        dteam['location'] = None
        dteam['stadium_id'] = None

    # competitions
    comp_file = data_dir / 'D_Competition.csv'
    if comp_file.exists():
        dcomp = pd.read_csv(comp_file)
        # robust mapping: map lowercased column names back to originals
        lower_map = {c.lower(): c for c in dcomp.columns}
        name_col_l = pick_col(list(lower_map.keys()), ['competition', 'name'])
        if name_col_l and name_col_l in lower_map:
            orig = lower_map[name_col_l]
            dcomp = dcomp.rename(columns={orig: 'competition'})
        else:
            dcomp = dcomp.rename(columns={dcomp.columns[0]: 'competition'})
        dcomp = dcomp[['competition']].drop_duplicates().reset_index(drop=True)
        dcomp['id_competition'] = range(1, len(dcomp)+1)
    else:
        comps = matches_df['competition'].fillna('ligue_1').unique()
        dcomp = pd.DataFrame({'competition': sorted([str(c) for c in comps if c is not None])})
        dcomp['id_competition'] = range(1, len(dcomp)+1)

    # seasons from D_Season.csv if present
    season_file = data_dir / 'D_Season.csv'
    if season_file.exists():
        dseason = pd.read_csv(season_file)
        # preserve original season info and any extra columns (e.g. BeforeAfterIndependencies)
        lower_map = {c.lower(): c for c in dseason.columns}
        s_col_l = pick_col(list(lower_map.keys()), ['season', 'season_name'])
        id_col_l = pick_col(list(lower_map.keys()), ['season_id', 'id_season', 'id'])
        if s_col_l and id_col_l and s_col_l in lower_map and id_col_l in lower_map:
            orig_s = lower_map[s_col_l]
            orig_id = lower_map[id_col_l]
            # rename but keep all other columns (so BeforeAfterIndependencies is preserved)
            dseason = dseason.rename(columns={orig_s: 'season', orig_id: 'season_id'})
            # ensure season_id is unique key
            dseason = dseason.drop_duplicates(subset=['season_id']).reset_index(drop=True)
        else:
            dseason = pd.DataFrame({'season': matches_df['season'].dropna().unique()})
            dseason = dseason.reset_index().rename(columns={'index':'season_id'})
            dseason['season_id'] = dseason['season_id'] + 1
    else:
        dseason = pd.DataFrame({'season': matches_df['season'].dropna().unique()})
        dseason = dseason.reset_index().rename(columns={'index':'season_id'})
        dseason['season_id'] = dseason['season_id'] + 1

    # stadiums
    stad_file = data_dir / 'D_Stadium.csv'
    if stad_file.exists():
        dstad = pd.read_csv(stad_file)
        lower_map = {c.lower(): c for c in dstad.columns}
        name_col_l = pick_col(list(lower_map.keys()), ['stadium', 'venue', 'name'])
        if name_col_l and name_col_l in lower_map:
            orig = lower_map[name_col_l]
            dstad = dstad.rename(columns={orig: 'stadium_name'})
        else:
            dstad = dstad.rename(columns={dstad.columns[0]: 'stadium_name'})
        dstad = dstad[['stadium_name']].drop_duplicates().reset_index(drop=True)
        dstad['id_stadium'] = range(1, len(dstad)+1)
    else:
        venues = matches_df['venue'].dropna().unique()
        dstad = pd.DataFrame({'stadium_name': sorted(venues)})
        dstad['id_stadium'] = range(1, len(dstad)+1)

    # dates dimension
    dates = []
    for d in matches_df['date_parsed'].dropna().unique():
        dates.append(pd.Timestamp(d))
    if len(dates)==0:
        ddate = pd.DataFrame(columns=['id_date','date','year','month','day','date_iso'])
    else:
        ddate = pd.DataFrame({'date': sorted(dates)})
        ddate['id_date'] = range(1, len(ddate)+1)
        ddate['date_iso'] = ddate['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        ddate['year'] = ddate['date'].dt.year
        ddate['month'] = ddate['date'].dt.month
        ddate['day'] = ddate['date'].dt.day

    return dteam, dcomp, dseason, dstad, ddate


def build_fact(matches_df, dteam, dcomp, dseason, dstad, ddate):
    # Enhanced team mapping with fuzzy matching
    def find_team_id_robust(team_name, team_map, team_list):
        """Find team ID with fallback to fuzzy matching"""
        if pd.isna(team_name):
            return -1, None
        team_str = str(team_name).strip()
        
        # Direct match
        if team_str in team_map:
            return team_map[team_str], team_str
        
        # Fuzzy: normalize accents and case
        import unicodedata
        def normalize(s):
            return ''.join(c for c in unicodedata.normalize('NFD', s.lower()) if unicodedata.category(c) != 'Mn')
        
        normalized_input = normalize(team_str)
        for team_name_orig in team_list:
            if normalize(team_name_orig) == normalized_input:
                return team_map[team_name_orig], team_name_orig
        
        # Fuzzy: contains
        for team_name_orig in team_list:
            if normalize(team_name_orig) in normalized_input or normalized_input in normalize(team_name_orig):
                return team_map[team_name_orig], team_name_orig
        
        return -1, team_str
    
    # maps
    team_map = dict(zip(dteam['team_name'], dteam['id_team']))
    team_list = list(dteam['team_name'])
    comp_map = dict(zip(dcomp['competition'], dcomp['id_competition']))
    season_map = dict(zip(dseason['season'], dseason['season_id']))
    stad_map = dict(zip(dstad['stadium_name'], dstad['id_stadium']))
    date_map = dict(zip(ddate['date_iso'], ddate['id_date']))

    # First pass: collect missing teams
    missing_teams = set()
    for team_name in matches_df['home_team_name']:
        team_id, _ = find_team_id_robust(team_name, team_map, team_list)
        if team_id == -1 and pd.notna(team_name):
            missing_teams.add(str(team_name).strip())
    
    for team_name in matches_df['away_team_name']:
        team_id, _ = find_team_id_robust(team_name, team_map, team_list)
        if team_id == -1 and pd.notna(team_name):
            missing_teams.add(str(team_name).strip())
    
    # Add missing teams to dteam
    if missing_teams:
        print(f"Adding {len(missing_teams)} missing teams to D_Team...")
        max_id = dteam['id_team'].max()
        new_rows = []
        for i, team_name in enumerate(sorted(missing_teams)):
            new_rows.append({
                'id_team': max_id + i + 1,
                'team_name': team_name,
                'location': None,
                'stadium_id': None
            })
        new_teams_df = pd.DataFrame(new_rows)
        dteam = pd.concat([dteam, new_teams_df], ignore_index=True)
        
        # Update team_map with new teams
        team_map = dict(zip(dteam['team_name'], dteam['id_team']))
        team_list = list(dteam['team_name'])
        print(f"  {len(missing_teams)} new teams added")
        for row in new_rows:
            print(f"    id_team={row['id_team']}: {row['team_name']}")

    f = pd.DataFrame()
    f['id_match'] = matches_df['id_match'].astype(str)
    # date parsed to iso
    f['date_iso'] = matches_df['date_parsed'].dt.strftime('%Y-%m-%d %H:%M:%S')
    f['id_date'] = f['date_iso'].map(date_map).fillna(-1).astype(int)

    # Use robust mapping for teams
    f['id_home_team'] = matches_df['home_team_name'].apply(lambda x: find_team_id_robust(x, team_map, team_list)[0])
    f['id_away_team'] = matches_df['away_team_name'].apply(lambda x: find_team_id_robust(x, team_map, team_list)[0])
    f['id_competition'] = matches_df['competition'].map(comp_map).fillna(-1).astype(int)
    f['season_id'] = matches_df['season'].map(season_map).fillna(-1).astype(int)

    # Map id_stadium from home_team's stadium (if available)
    f['id_stadium'] = f['id_home_team'].map(lambda hid: dteam[dteam['id_team'] == hid]['stadium_id'].iloc[0] if hid != -1 and len(dteam[dteam['id_team'] == hid]) > 0 and pd.notna(dteam[dteam['id_team'] == hid]['stadium_id'].iloc[0]) else None)

    f['stage'] = matches_df['stage']
    f['status'] = matches_df['status']
    f['result_home'] = pd.to_numeric(matches_df['result_home'], errors='coerce').fillna(-1).astype(int)
    f['result_away'] = pd.to_numeric(matches_df['result_away'], errors='coerce').fillna(-1).astype(int)
    f['regulation_time'] = matches_df['regulation_time']
    f['penalties'] = matches_df['penalties']
    f['venue'] = matches_df['venue']

    # ensure columns order matching requested CREATE TABLE (removed capacity column)
    cols = ['id_match','id_date','id_home_team','id_away_team','id_competition','season_id','id_stadium','stage','status','result_home','result_away','regulation_time','penalties','venue']
    f = f[cols]
    return f, dteam


def load_topscorers_dimensions(data_dir, dteam):
    """Load and clean D_TopScorers_AllTime and D_TopScorers_By_Season"""
    
    # Créer mappings robustes pour les noms d'équipes
    # Mapping manuel pour corriger les variations d'accents et de formatage
    manual_team_map = {
        'Espérance de Tunis': 'Esperance Tunis',
        'Étoile du Sahel': 'Etoile du Sahel',
        'JS Métouia': 'JS Metlaoui',  
        'OC Kerkennah': 'Océano Club Kerkennah',
        'Olympique des Transports': 'SC Moknine',  
        'Olympique du Kef': 'Jendouba Sport',  
        'Sfax Railways Sports': 'SFAX RAIL',
        'Sfax Rail': 'SFAX RAIL',
        'Club de Hammam-Lif': 'CS Hammam-Lif',
        'Club Bizertin': 'CA Bizertin',
        'Olympique Béja': 'Olympique Beja',
        'Avenir de Marsa': 'Avenir Sportif de La Marsa',
        'Jeunesse Kairouanaise': 'JS Kairouan',
        'US Tunis': 'US Tataouine',  # approximation
        'Tunisia Haykel Guemamdia': 'AS Gabès',
        'Tunisia Taieb Ben Zitoun': 'AS Gabès',
    }
    
    # exact match + fuzzy match (removes prefixes like "Tunisia", "Nigeria", etc)
    def find_team_id(team_name_str, team_map, dteam_df):
        if pd.isna(team_name_str):
            return -1
        team_str = str(team_name_str).strip()
        # exact match
        if team_str in team_map:
            return team_map[team_str]
        # apply manual mapping
        if team_str in manual_team_map:
            mapped_name = manual_team_map[team_str]
            if mapped_name in team_map:
                return team_map[mapped_name]
        # remove country prefix (e.g. "Tunisia X" -> "X")
        parts = team_str.split()
        if len(parts) > 1:
            # check if first part is a country name, remove it
            name_without_prefix = ' '.join(parts[1:])
            if name_without_prefix in team_map:
                return team_map[name_without_prefix]
        # fuzzy match: look for contains
        for team_name, tid in team_map.items():
            if team_name.lower() in team_str.lower() or team_str.lower() in team_name.lower():
                return tid
        return -1
    
    # D_TopScorers_AllTime
    dtopscore_all = None
    alltimefile = data_dir / 'D_TopScorers_AllTime.csv'
    if alltimefile.exists():
        try:
            dtopscore_all = pd.read_csv(alltimefile)
            # normalize columns
            lower_map = {c.lower(): c for c in dtopscore_all.columns}
            name_col_l = pick_col(list(lower_map.keys()), ['name', 'player_name'])
            goals_col_l = pick_col(list(lower_map.keys()), ['goals', 'goal_count'])
            team_col_l = pick_col(list(lower_map.keys()), ['team', 'team_name'])
            id_team_col_l = pick_col(list(lower_map.keys()), ['id_team', 'team_id'])
            
            renames = {}
            if name_col_l and name_col_l in lower_map:
                renames[lower_map[name_col_l]] = 'player_name'
            if goals_col_l and goals_col_l in lower_map:
                renames[lower_map[goals_col_l]] = 'goals'
            if team_col_l and team_col_l in lower_map:
                renames[lower_map[team_col_l]] = 'team_name_raw'
            if id_team_col_l and id_team_col_l in lower_map:
                renames[lower_map[id_team_col_l]] = 'id_team'
            
            dtopscore_all = dtopscore_all.rename(columns=renames)
            
            # Build team map for mapping
            team_map = dict(zip(dteam['team_name'], dteam['id_team']))
            
            # Clean team names: extract primary team name (first one in format "Team1 (x), Team2 (y)")
            if 'team_name_raw' in dtopscore_all.columns:
                def extract_primary_team(team_str):
                    if pd.isna(team_str) or str(team_str).strip() == '':
                        return None
                    team_str = str(team_str).strip()
                    # if contains comma, take first part
                    if ',' in team_str:
                        team_str = team_str.split(',')[0].strip()
                    # if contains parenthesis, remove goals count (e.g. "Team (80)" -> "Team")
                    if '(' in team_str and ')' in team_str:
                        team_str = team_str[:team_str.index('(')].strip()
                    return team_str if team_str else None
                
                dtopscore_all['team_name_cleaned'] = dtopscore_all['team_name_raw'].apply(extract_primary_team)
                # Use source id_team if valid, otherwise use mapped id
                def map_id_with_source(row):
                    # If source has a valid id_team, use it
                    if pd.notna(row['id_team']) and row['id_team'] > 0:
                        return int(row['id_team'])
                    # Otherwise, map from team name
                    if pd.notna(row['team_name_cleaned']):
                        return find_team_id(row['team_name_cleaned'], team_map, dteam)
                    return -1
                
                dtopscore_all['id_team'] = dtopscore_all.apply(map_id_with_source, axis=1)
                dtopscore_all = dtopscore_all.drop(columns=['team_name_raw', 'team_name_cleaned'])
            else:
                # ensure id_team exists and is int
                if 'id_team' not in dtopscore_all.columns:
                    dtopscore_all['id_team'] = -1
                dtopscore_all['id_team'] = dtopscore_all['id_team'].fillna(-1).astype(int)
            
            # convert goals to int
            if 'goals' in dtopscore_all.columns:
                dtopscore_all['goals'] = pd.to_numeric(dtopscore_all['goals'], errors='coerce').fillna(0).astype(int)
            
            # create id_topscorer
            dtopscore_all['id_topscorer'] = range(1, len(dtopscore_all) + 1)
            dtopscore_all = dtopscore_all[['id_topscorer', 'player_name', 'id_team', 'goals']].drop_duplicates()
            print(f'Loaded {len(dtopscore_all)} all-time top scorers')
        except Exception as e:
            print(f'Failed to load D_TopScorers_AllTime: {e}')
    
    # D_TopScorers_By_Season
    dtopscore_season = None
    seasonfile = data_dir / 'D_TopScorers_By_Season.csv'
    if seasonfile.exists():
        try:
            dtopscore_season = pd.read_csv(seasonfile)
            # normalize columns
            lower_map = {c.lower(): c for c in dtopscore_season.columns}
            season_col_l = pick_col(list(lower_map.keys()), ['season', 'season_name'])
            seasonid_col_l = pick_col(list(lower_map.keys()), ['season_id', 'id_season'])
            name_col_l = pick_col(list(lower_map.keys()), ['name', 'player_name'])
            goals_col_l = pick_col(list(lower_map.keys()), ['goals', 'goal_count'])
            team_col_l = pick_col(list(lower_map.keys()), ['team', 'team_name'])
            id_team_col_l = pick_col(list(lower_map.keys()), ['id_team', 'team_id'])
            
            renames = {}
            if season_col_l and season_col_l in lower_map:
                renames[lower_map[season_col_l]] = 'season'
            if seasonid_col_l and seasonid_col_l in lower_map:
                renames[lower_map[seasonid_col_l]] = 'season_id'
            if name_col_l and name_col_l in lower_map:
                renames[lower_map[name_col_l]] = 'player_name'
            if goals_col_l and goals_col_l in lower_map:
                renames[lower_map[goals_col_l]] = 'goals'
            if team_col_l and team_col_l in lower_map:
                renames[lower_map[team_col_l]] = 'team_name_raw'
            if id_team_col_l and id_team_col_l in lower_map:
                renames[lower_map[id_team_col_l]] = 'id_team'
            
            dtopscore_season = dtopscore_season.rename(columns=renames)
            
            # Build team map for mapping
            team_map = dict(zip(dteam['team_name'], dteam['id_team']))
            
            # Clean team names: extract primary team name
            if 'team_name_raw' in dtopscore_season.columns:
                def extract_primary_team(team_str):
                    if pd.isna(team_str) or str(team_str).strip() == '':
                        return None
                    team_str = str(team_str).strip()
                    # if contains comma, take first part
                    if ',' in team_str:
                        team_str = team_str.split(',')[0].strip()
                    # if contains parenthesis, remove goals count
                    if '(' in team_str and ')' in team_str:
                        team_str = team_str[:team_str.index('(')].strip()
                    return team_str if team_str else None
                
                dtopscore_season['team_name_cleaned'] = dtopscore_season['team_name_raw'].apply(extract_primary_team)
                # Use source id_team if valid, otherwise use mapped id
                def map_id_with_source(row):
                    # If source has a valid id_team, use it
                    if pd.notna(row['id_team']) and row['id_team'] > 0:
                        return int(row['id_team'])
                    # Otherwise, map from team name
                    if pd.notna(row['team_name_cleaned']):
                        return find_team_id(row['team_name_cleaned'], team_map, dteam)
                    return -1
                
                dtopscore_season['id_team'] = dtopscore_season.apply(map_id_with_source, axis=1)
                dtopscore_season = dtopscore_season.drop(columns=['team_name_raw', 'team_name_cleaned'])
            else:
                # ensure id_team exists and is int
                if 'id_team' not in dtopscore_season.columns:
                    dtopscore_season['id_team'] = -1
                dtopscore_season['id_team'] = dtopscore_season['id_team'].fillna(-1).astype(int)
            
            # convert goals to int
            if 'goals' in dtopscore_season.columns:
                dtopscore_season['goals'] = pd.to_numeric(dtopscore_season['goals'], errors='coerce').fillna(0).astype(int)
            
            # ensure season_id is int
            if 'season_id' in dtopscore_season.columns:
                dtopscore_season['season_id'] = pd.to_numeric(dtopscore_season['season_id'], errors='coerce').fillna(-1).astype(int)
            
            # create id_topscorer_season
            dtopscore_season['id_topscorer_season'] = range(1, len(dtopscore_season) + 1)
            dtopscore_season = dtopscore_season[['id_topscorer_season', 'season_id', 'player_name', 'id_team', 'goals']].drop_duplicates()
            print(f'Loaded {len(dtopscore_season)} season top scorers')
        except Exception as e:
            print(f'Failed to load D_TopScorers_By_Season: {e}')
    
    return dtopscore_all, dtopscore_season


def build_team_season_agg(fmatch):
    """Generate F_Team_Season aggregated table with stats per team/season"""
    
    records = []
    
    for _, row in fmatch.iterrows():
        season_id = row['season_id']
        
        # Skip if no valid season
        if pd.isna(season_id) or season_id == -1:
            continue
        
        home_team_id = row['id_home_team']
        home_goals = row['result_home']
        away_goals = row['result_away']
        
        # HOME TEAM STATS
        if not pd.isna(home_team_id) and home_team_id != -1:
            if pd.isna(home_goals) or pd.isna(away_goals):
                home_points = 0
            else:
                home_goals_int = int(home_goals)
                away_goals_int = int(away_goals)
                if home_goals_int > away_goals_int:
                    home_points = 3
                elif home_goals_int == away_goals_int:
                    home_points = 1
                else:
                    home_points = 0
            
            records.append({
                'season_id': int(season_id),
                'id_team': int(home_team_id),
                'matches_home': 1,
                'matches_away': 0,
                'goals_for_home': int(home_goals) if not pd.isna(home_goals) else 0,
                'goals_for_away': 0,
                'goals_against_home': int(away_goals) if not pd.isna(away_goals) else 0,
                'goals_against_away': 0,
                'points_home': home_points,
                'points_away': 0,
                'wins_home': 1 if (not pd.isna(home_goals) and not pd.isna(away_goals) and int(home_goals) > int(away_goals)) else 0,
                'wins_away': 0,
                'draws_home': 1 if (not pd.isna(home_goals) and not pd.isna(away_goals) and int(home_goals) == int(away_goals)) else 0,
                'draws_away': 0,
                'losses_home': 1 if (not pd.isna(home_goals) and not pd.isna(away_goals) and int(home_goals) < int(away_goals)) else 0,
                'losses_away': 0,
            })
        
        # AWAY TEAM STATS
        away_team_id = row['id_away_team']
        if not pd.isna(away_team_id) and away_team_id != -1:
            if pd.isna(home_goals) or pd.isna(away_goals):
                away_points = 0
            else:
                home_goals_int = int(home_goals)
                away_goals_int = int(away_goals)
                if away_goals_int > home_goals_int:
                    away_points = 3
                elif away_goals_int == home_goals_int:
                    away_points = 1
                else:
                    away_points = 0
            
            records.append({
                'season_id': int(season_id),
                'id_team': int(away_team_id),
                'matches_home': 0,
                'matches_away': 1,
                'goals_for_home': 0,
                'goals_for_away': int(away_goals) if not pd.isna(away_goals) else 0,
                'goals_against_home': 0,
                'goals_against_away': int(home_goals) if not pd.isna(home_goals) else 0,
                'points_home': 0,
                'points_away': away_points,
                'wins_home': 0,
                'wins_away': 1 if (not pd.isna(home_goals) and not pd.isna(away_goals) and int(away_goals) > int(home_goals)) else 0,
                'draws_home': 0,
                'draws_away': 1 if (not pd.isna(home_goals) and not pd.isna(away_goals) and int(away_goals) == int(home_goals)) else 0,
                'losses_home': 0,
                'losses_away': 1 if (not pd.isna(home_goals) and not pd.isna(away_goals) and int(away_goals) < int(home_goals)) else 0,
            })
    
    df_records = pd.DataFrame(records)
    
    # Aggregate by season_id + id_team
    agg_dict = {
        'matches_home': 'sum',
        'matches_away': 'sum',
        'goals_for_home': 'sum',
        'goals_for_away': 'sum',
        'goals_against_home': 'sum',
        'goals_against_away': 'sum',
        'points_home': 'sum',
        'points_away': 'sum',
        'wins_home': 'sum',
        'wins_away': 'sum',
        'draws_home': 'sum',
        'draws_away': 'sum',
        'losses_home': 'sum',
        'losses_away': 'sum',
    }
    
    f_team_season = df_records.groupby(['season_id', 'id_team'], as_index=False).agg(agg_dict)
    
    # Compute totals and averages
    f_team_season['matches_total'] = f_team_season['matches_home'] + f_team_season['matches_away']
    f_team_season['goals_for'] = f_team_season['goals_for_home'] + f_team_season['goals_for_away']
    f_team_season['goals_against'] = f_team_season['goals_against_home'] + f_team_season['goals_against_away']
    f_team_season['goals_diff'] = f_team_season['goals_for'] - f_team_season['goals_against']
    f_team_season['points'] = f_team_season['points_home'] + f_team_season['points_away']
    f_team_season['wins'] = f_team_season['wins_home'] + f_team_season['wins_away']
    f_team_season['draws'] = f_team_season['draws_home'] + f_team_season['draws_away']
    f_team_season['losses'] = f_team_season['losses_home'] + f_team_season['losses_away']
    f_team_season['goals_per_match'] = (f_team_season['goals_for'] / f_team_season['matches_total']).round(2)
    f_team_season['goals_against_per_match'] = (f_team_season['goals_against'] / f_team_season['matches_total']).round(2)
    
    # Select and reorder columns
    cols_order = [
        'season_id', 'id_team',
        'matches_total', 'matches_home', 'matches_away',
        'wins', 'draws', 'losses',
        'wins_home', 'wins_away',
        'draws_home', 'draws_away',
        'losses_home', 'losses_away',
        'points', 'points_home', 'points_away',
        'goals_for', 'goals_against', 'goals_diff',
        'goals_for_home', 'goals_for_away',
        'goals_against_home', 'goals_against_away',
        'goals_per_match', 'goals_against_per_match'
    ]
    
    f_team_season = f_team_season[cols_order]
    
    # Ensure proper data types
    f_team_season['season_id'] = f_team_season['season_id'].astype(int)
    f_team_season['id_team'] = f_team_season['id_team'].astype(int)
    
    for col in ['matches_total', 'matches_home', 'matches_away', 'wins', 'draws', 'losses',
                'wins_home', 'wins_away', 'draws_home', 'draws_away', 'losses_home', 'losses_away',
                'points', 'points_home', 'points_away',
                'goals_for', 'goals_against', 'goals_diff',
                'goals_for_home', 'goals_for_away',
                'goals_against_home', 'goals_against_away']:
        f_team_season[col] = f_team_season[col].astype(int)
    
    return f_team_season


def main():
    print('Scanning matches...')
    files = glob.glob(MATCHES_GLOB, recursive=True)
    print(f'Found {len(files)} match files')
    dfs = []
    for p in sorted(files):
        try:
            dfm = read_match_file(p)
            # parse dates
            dfm['date_parsed'] = dfm['date_raw'].apply(parse_date_safe)
            dfs.append(dfm)
        except Exception as e:
            print('Failed reading', p, e)
    if len(dfs)==0:
        print('No match data found. Exiting.')
        return
    matches = pd.concat(dfs, ignore_index=True)

    # build dimensions
    print('Building dimensions...')
    dteam, dcomp, dseason, dstad, ddate = build_dimensions(matches, DATA_DIR)

    # load topscorers dimensions
    print('Loading topscorers dimensions...')
    dtopscore_all, dtopscore_season = load_topscorers_dimensions(DATA_DIR, dteam)

    # prepare matches with parsed dates
    matches['date_parsed'] = pd.to_datetime(matches['date_parsed'])
    print('Building fact table F_Match...')
    fmatch, dteam = build_fact(matches, dteam, dcomp, dseason, dstad, ddate)
    
    # Clean up stadium_id: replace -1 with None and convert to Int64 (nullable int)
    dteam['stadium_id'] = dteam['stadium_id'].replace(-1, None)
    dteam['stadium_id'] = dteam['stadium_id'].astype('Int64')
    
    # save dimensions (after build_fact to include new teams)
    dteam.to_csv(OUTPUT_DIR / 'D_Team_clean.csv', index=False)
    dcomp.to_csv(OUTPUT_DIR / 'D_Competition_clean.csv', index=False)
    dseason.to_csv(OUTPUT_DIR / 'D_Season_clean.csv', index=False)
    dstad.to_csv(OUTPUT_DIR / 'D_Stadium_clean.csv', index=False)
    ddate.to_csv(OUTPUT_DIR / 'D_Date.csv', index=False)
    if dtopscore_all is not None:
        dtopscore_all.to_csv(OUTPUT_DIR / 'D_TopScorers_AllTime_clean.csv', index=False)
    if dtopscore_season is not None:
        dtopscore_season.to_csv(OUTPUT_DIR / 'D_TopScorers_By_Season_clean.csv', index=False)
    
    # Save updated D_Team with new teams
    dteam.to_csv(OUTPUT_DIR / 'D_Team_clean.csv', index=False)
    fmatch.to_csv(OUTPUT_DIR / 'F_Match.csv', index=False)

    # Generate F_Team_Season aggregated table
    print('Generating F_Team_Season aggregated table...')
    fmatch_reload = pd.read_csv(OUTPUT_DIR / 'F_Match.csv')
    f_team_season = build_team_season_agg(fmatch_reload)
    f_team_season.to_csv(OUTPUT_DIR / 'F_Team_Season.csv', index=False)
    print(f'  Generated {len(f_team_season)} team-season records')

    # champions: copy/clean D_Champions.csv if exists
    champions_file = DATA_DIR / 'D_Champions.csv'
    if champions_file.exists():
        try:
            dchamp = pd.read_csv(champions_file)
            dchamp.to_csv(OUTPUT_DIR / 'D_Champions_clean.csv', index=False)
            print('D_Champions copied to output')
        except Exception:
            print('Failed to copy D_Champions')

    print('ETL complete. CSVs saved to', OUTPUT_DIR)

if __name__ == '__main__':
    main()
