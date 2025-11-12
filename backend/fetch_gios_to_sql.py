import os
import requests
import pyodbc
from dotenv import load_dotenv

# 1️⃣ Wczytanie danych z .env
load_dotenv()
AZURE_SERVER = os.getenv("AZURE_SERVER")
AZURE_DB = os.getenv("AZURE_DB")
AZURE_USER = os.getenv("AZURE_USER")
AZURE_PASS = os.getenv("AZURE_PASS")

# 2️⃣ Ustawienia połączenia z Azure SQL
conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={AZURE_SERVER};DATABASE={AZURE_DB};"
    f"UID={AZURE_USER};PWD={AZURE_PASS};Encrypt=yes;TrustServerCertificate=no;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# 3️⃣ Pobranie listy stacji
url_stacje = "https://api.gios.gov.pl/pjp-api/v1/rest/station/findAll"
resp = requests.get(url_stacje, timeout=15)
resp.raise_for_status()
stacje = resp.json()["Lista stacji pomiarowych"]

print(f"✅ Znaleziono {len(stacje)} stacji GIOŚ")

# 4️⃣ Iteracja po kilku stacjach (na początek 3, potem można zwiększyć)
for st in stacje[:3]:
    station_id = st["Identyfikator stacji"]
    station_name = st["Nazwa stacji"]

    print(f"\n📡 Stacja: {station_name} (ID: {station_id})")

    # 🔹 Lista czujników dla danej stacji
    sensors_url = f"https://api.gios.gov.pl/pjp-api/v1/rest/station/sensors/{station_id}"
    sensors_resp = requests.get(sensors_url, timeout=15)
    sensors_resp.raise_for_status()
    sensors_data = sensors_resp.json()
    sensors = sensors_data["Lista stanowisk pomiarowych dla podanej stacji"]

    for s in sensors:
        param_code = s["Wskaźnik - kod"]
        if param_code not in ["PM10", "PM2.5"]:
            continue

        sensor_id = s["Identyfikator stanowiska"]
        print(f"  ↳ Czujnik {param_code} (ID: {sensor_id})")

        # 🔹 Dane z czujnika
        data_url = f"https://api.gios.gov.pl/pjp-api/v1/rest/data/getData/{sensor_id}"
        data_resp = requests.get(data_url, timeout=15)
        if data_resp.status_code != 200:
            print(f"    ⚠️ Brak danych dla {param_code}")
            continue

        data_json = data_resp.json()
        pomiary = data_json.get("Lista danych pomiarowych", [])

        # 🔹 Zapis do bazy Azure SQL
        for m in pomiary:
            wartosc = m.get("Wartość")
            data_pomiaru = m.get("Data")
            if wartosc is None or data_pomiaru is None:
                continue

            cursor.execute("""
                INSERT INTO dbo.Measurements (StationId, Timestamp, PM10, PM25)
                VALUES (?, ?, ?, ?)
            """, station_id, data_pomiaru,
                 wartosc if param_code == "PM10" else None,
                 wartosc if param_code == "PM2.5" else None)

        conn.commit()
        print(f"    💾 Zapisano {len(pomiary)} rekordów ({param_code})")

print("\n✅ Zakończono synchronizację danych GIOŚ → Azure SQL")
conn.close()
