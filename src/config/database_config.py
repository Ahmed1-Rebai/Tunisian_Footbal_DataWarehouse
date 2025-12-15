# Configuration Database - SSMS Connection
# Cette file contient les paramètres de connexion à SQL Server

import pyodbc

# SQL Server Configuration
SQL_SERVER_CONFIG = {
    'server': r'DESKTOP-MMF34HS\MSSQLSERVER01',
    'driver': '{ODBC Driver 17 for SQL Server}',  # Ou {ODBC Driver 18 for SQL Server} pour versions plus récentes
    'database': 'DW_Tunisia_Football',  # Remplacer par votre nom de base de données
    'trusted_connection': True,  # Windows Authentication
    # Alternative avec authentification SQL:
    # 'user': 'sa',
    # 'password': 'votre_password',
}

def get_connection_string():
    """Retourne la chaîne de connexion SQL Server"""
    if 'trusted_connection' in SQL_SERVER_CONFIG and SQL_SERVER_CONFIG['trusted_connection']:
        return f"DRIVER={SQL_SERVER_CONFIG['driver']};SERVER={SQL_SERVER_CONFIG['server']};DATABASE={SQL_SERVER_CONFIG['database']};Trusted_Connection=yes;"
    else:
        return f"DRIVER={SQL_SERVER_CONFIG['driver']};SERVER={SQL_SERVER_CONFIG['server']};DATABASE={SQL_SERVER_CONFIG['database']};UID={SQL_SERVER_CONFIG['user']};PWD={SQL_SERVER_CONFIG['password']};"

def connect_to_ssms():
    """Établit une connexion à SQL Server"""
    try:
        conn = pyodbc.connect(get_connection_string())
        print("✓ Connexion à SQL Server réussie")
        return conn
    except pyodbc.Error as e:
        print(f"✗ Erreur de connexion: {e}")
        return None

if __name__ == "__main__":
    # Test de connexion
    conn = connect_to_ssms()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT @@version")
        print(cursor.fetchone()[0])
        conn.close()
