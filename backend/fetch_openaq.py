import os
import pyodbc
import requests
from dotenv import load_dotenv

# 🔹 Wczytaj dane logowania z pliku .env
load_dotenv()
AZURE_SERVER = os.getenv("AZURE_SERVER")
AZURE_DB = os.getenv("AZURE_DB")
AZURE_USER = os.getenv("AZURE_USER")
AZURE_PASS = os.getenv("AZURE_PASS")

# 🔹 Połączenie z bazą Azure SQL
conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={AZURE_SERVER};DATABASE={AZURE_DB};"
    f"UID={AZURE_USER};PWD={AZURE_PASS};Encrypt=yes;TrustServerCertificate=no;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# 🔹 Pobieranie danych z GIOŚ
url_stacje = "https://api.gios.gov.pl/pjp-api/v1/rest/station/findAll"
resp = requests.get(url_stacje, timeout=10)
resp.raise_for_status()
stacje = resp.json()["Lista stacji pomiarowych"]

print(f"✅ Znaleziono {len(stacje)} stacji.")

for st in stacje[:5]:  # ← pobierz tylko 5 stacji na próbę
    station_id = st["Identyfikator stacji"]
    station_name = st["Nazwa stacji"]

    # 🔹 Zapytanie o czujniki
    url_sensory = f"https://api.gios.gov.pl/pjp-api/v1/rest/station/sensors/{station_id}"
    sensors = requests.get(url_sensory).json()["Lista stanowisk pomiarowych dla podanej stacji"]

    for s in sensors:
        if s["Wskaźnik - kod"] in ["PM10", "PM2.5"]:
            sensor_id = s["Identyfikator stanowiska"]

            # 🔹 Dane pomiarowe
            url_data = f"https://api.gios.gov.pl/pjp-api/v1/rest/data/getData/{sensor_id}"
            data_resp = requests.get(url_data)
            if data_resp.status_code != 200:
                continue

            data_json = data_resp.json()
            measurements = data_json.get("Lista danych pomiarowych", [])

            # 🔹 Zapisz dane do SQL
            for m in measurements:
                value = m.get("Wartość")
                timestamp = m.get("Data")
                if value is None or timestamp is None:
                    continue

                if s["Wskaźnik - kod"] == "PM10":
                    cursor.execute("""
                        INSERT INTO dbo.Measurements (StationId, Timestamp, PM10)
                        VALUES (?, ?, ?)
                    """, station_id, timestamp, value)
                elif s["Wskaźnik - kod"] == "PM2.5":
                    cursor.execute("""
                        INSERT INTO dbo.Measurements (StationId, Timestamp, PM25)
                        VALUES (?, ?, ?)
                    """, station_id, timestamp, value)

            conn.commit()
            print(f"📥 Wstawiono dane dla stacji {station_name} ({station_id}) – {s['Wskaźnik - kod']}")

print("✅ Zakończono zapisywanie danych.")
conn.close()
