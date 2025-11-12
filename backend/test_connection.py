import pyodbc
import os
from dotenv import load_dotenv

# 🔹 Wczytanie zmiennych środowiskowych z pliku .env
load_dotenv()

server = os.getenv("AZURE_SERVER")
database = os.getenv("AZURE_DB")
username = os.getenv("AZURE_USER")
password = os.getenv("AZURE_PASS")

# 🔹 Łączenie z bazą danych
try:
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    print("🔗 Próba połączenia z bazą danych Azure SQL...")
    conn = pyodbc.connect(conn_str)
    print("✅ Połączenie z Azure SQL działa poprawnie!")

    cursor = conn.cursor()
    cursor.execute("SELECT TOP 1 * FROM dbo.Stations;")
    row = cursor.fetchone()
    print("📊 Przykładowy rekord z tabeli Stations:", row)

    conn.close()

except Exception as e:
    print("❌ Błąd połączenia:", e)
