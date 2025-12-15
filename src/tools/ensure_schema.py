"""
Script d'usage rapide pour forcer la validation et produire un rapport simplifié.
Usage:
  python src/tools/ensure_schema.py --dir warehouse_output

Retourne code 0 si tout est OK, 2 si erreurs trouvées.
"""
import sys
from .validate_schema import validate_all_in_directory

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', default='warehouse_output')
    args = parser.parse_args()

    res = validate_all_in_directory(args.dir)
    exit_code = 0
    for fname, detail in res.items():
        if not detail.get('ok'):
            print(f"{fname}: FAIL")
            for e in detail.get('errors', []):
                print("  -", e)
            for w in detail.get('warnings', []):
                print("  ~", w)
            exit_code = 2
        else:
            print(f"{fname}: OK")
    sys.exit(exit_code)
