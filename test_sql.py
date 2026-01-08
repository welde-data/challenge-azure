import json
import pyodbc
import os

# 1. Load configuration safely
if os.path.exists("local.settings.json"):
    # Local development
    with open("local.settings.json", "r", encoding="utf-8") as f:
        v = json.load(f)["Values"]
else:
    # Azure Environment
    v = os.environ

# 2. Correct Connection String for Linux/Azure
# For Python 3.10, Driver 17 
driver = "{ODBC Driver 17 for SQL Server}"

connection_string = (
    f"Driver={driver};"
    f"Server=tcp:{v['SQL_SERVER']},1433;"
    f"Database={v['SQL_DATABASE']};"
    f"Uid={v['irail-db']};"
    f"Pwd={v['SQL_PASSWORD']};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

try:
    conn = pyodbc.connect(connection_string)
    print("Success: Connected to Azure SQL!")
    conn.close()
except pyodbc.Error as e:
    print(f"Error: {e}")