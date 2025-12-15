"""
Outils pour valider les CSV contre les définitions de schéma.
Usage:
  from src.config.schema_definitions import SCHEMA_DEFINITIONS
  from src.tools.validate_schema import validate_csv_file

  validate_csv_file("warehouse_output/D_Team_clean.csv", "D_Team")

Le script signale :
- colonnes manquantes requises
- colonnes supplémentaires (non bloquant)

Retourne dict {"ok": bool, "errors": [...], "warnings": [...]}.
"""

import os
import pandas as pd
from typing import Dict, List
from ..config.schema_definitions import SCHEMA_DEFINITIONS


def _normalize_col(c: str) -> str:
    return c.strip().lower().replace(' ', '_')


def validate_dataframe_columns(df: pd.DataFrame, table: str) -> Dict:
    table_def = SCHEMA_DEFINITIONS.get(table)
    if not table_def:
        return {"ok": False, "errors": [f"Table '{table}' non définie dans SCHEMA_DEFINITIONS."], "warnings": []}

    df_cols = {_normalize_col(c) for c in df.columns}
    required = {c.lower() for c in table_def.get('required', [])}
    all_cols = {c.lower() for c in table_def.get('all', [])}

    missing = sorted(list(required - df_cols))
    extra = sorted(list(df_cols - all_cols))

    errors = []
    warnings = []
    if missing:
        errors.append(f"Colonnes requises manquantes: {missing}")
    if extra:
        warnings.append(f"Colonnes non attendues (extra): {extra}")

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}


def validate_csv_file(path: str, table: str) -> Dict:
    if not os.path.exists(path):
        return {"ok": False, "errors": [f"Fichier non trouvé: {path}"], "warnings": []}

    try:
        df = pd.read_csv(path)
    except Exception as e:
        return {"ok": False, "errors": [f"Erreur lecture CSV: {e}"], "warnings": []}

    return validate_dataframe_columns(df, table)


def validate_all_in_directory(directory: str) -> Dict[str, Dict]:
    """Parcours `directory` et tente de valider chaque CSV en le mappant à une table via le nom de fichier."""
    results = {}
    for fname in os.listdir(directory):
        if not fname.lower().endswith('.csv'):
            continue
        key = os.path.splitext(fname)[0].lower()
        # tenter de deviner la table
        guessed_table = None
        for table_name, patterns in SCHEMA_DEFINITIONS.items():
            # on ne mappe pas ici; patterns lives in FILENAME_TO_TABLE (if needed)
            pass
        # fallback: table name equals key (normalisé en majuscule Camel)
        # ex: d_team_clean -> D_Team
        candidate = ''.join([p.capitalize() for p in key.split('_')])
        # try different common forms
        # simple heuristic: check SCHEMA_DEFINITIONS keys by lowercase match
        for table_name in SCHEMA_DEFINITIONS.keys():
            if table_name.lower() == candidate.lower():
                guessed_table = table_name
                break
        # if not found, try removing suffixes like _clean or _all
        if not guessed_table:
            short = key.replace('_clean', '').replace('_alltime', '').replace('_by_season','')
            for table_name in SCHEMA_DEFINITIONS.keys():
                if table_name.lower() == short.lower():
                    guessed_table = table_name
                    break
        if not guessed_table:
            results[fname] = {"ok": False, "errors": ["Impossible de deviner la table cible depuis le nom de fichier."], "warnings": []}
            continue
        results[fname] = validate_csv_file(os.path.join(directory, fname), guessed_table)
    return results


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Valide les CSV d\'un répertoire contre le schéma attendu.')
    parser.add_argument('--dir', default='warehouse_output', help='Dossier contenant les CSV à valider (défaut: warehouse_output)')
    args = parser.parse_args()

    res = validate_all_in_directory(args.dir)
    any_errors = False
    for f, r in res.items():
        print(f"== {f} ==")
        if r.get('errors'):
            any_errors = True
            for e in r['errors']:
                print("  ERROR:", e)
        if r.get('warnings'):
            for w in r['warnings']:
                print("  WARNING:", w)
        if r.get('ok'):
            print("  OK")
        print()

    if any_errors:
        print("Validation échouée: des erreurs ont été détectées.")
        raise SystemExit(2)
    else:
        print("Validation terminée: aucun problème bloquant détecté.")
