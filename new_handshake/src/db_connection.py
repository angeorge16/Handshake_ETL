import pyodbc
import yaml
import os

def get_config():
    with open(os.path.join(os.path.dirname(__file__), '../config/config.yaml')) as f:
        return yaml.safe_load(f)

def get_connection():
    config = get_config()['database']
    conn_str = (
        f"DRIVER={config['driver']};"
        f"SERVER={config['server']};"
        f"DATABASE={config['dbname']};"
        f"Trusted_Connection={'yes' if config['trusted_connection'] else 'no'};"
    )
    return pyodbc.connect(conn_str)
