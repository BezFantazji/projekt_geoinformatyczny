import os
import pyodbc
from dotenv import load_dotenv

# 1️⃣ Wczytanie danych z pliku .env
load_dotenv()
AZURE_SERVER = os.getenv("AZURE_SERVER")
AZURE_DB = os.getenv("AZURE_DB")
AZURE_USER = os.getenv("AZURE_USER")
AZURE_PASS = os.getenv("AZURE_PASS")

# 2️⃣ Połączenie z Azure SQL
conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={AZURE_SERVER};DATABASE={AZURE_DB};"
    f"UID={AZURE_USER};PWD={AZURE_PASS};Encrypt=yes;TrustServerCertificate=no;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

print("🔗 Połączono z bazą Azure SQL")

# 3️⃣ Funkcja do obliczania AQI wg norm WHO
def calculate_aqi(pm10, pm25):
    aqi_pm10 = None
    aqi_pm25 = None

    if pm10 is not None:
        if pm10 <= 20:
            aqi_pm10 = 0
        elif pm10 <= 50:
            aqi_pm10 = 1
        elif pm10 <= 100:
            aqi_pm10 = 2
        elif pm10 <= 200:
            aqi_pm10 = 3
        else:
            aqi_pm10 = 4

    if pm25 is not None:
        if pm25 <= 10:
            aqi_pm25 = 0
        elif pm25 <= 25:
            aqi_pm25 = 1
        elif pm25 <= 50:
            aqi_pm25 = 2
        elif pm25 <= 75:
            aqi_pm25 = 3
        else:
            aqi_pm25 = 4

    # 🧮 wybieramy gorszy wynik jako AQI całkowity
    if aqi_pm10 is not None and aqi_pm25 is not None:
        return max(aqi_pm10, aqi_pm25)
    return aqi_pm10 or aqi_pm25 or 0


# 4️⃣ Pobranie danych bez AQI
cursor.execute("""
    SELECT Id, PM10, PM25 
    FROM dbo.Measurements 
    WHERE AQI IS NULL
""")
records = cursor.fetchall()

print(f"📊 Znaleziono {len(records)} rekordów do aktualizacji AQI")

# 5️⃣ Obliczanie i zapis
updated = 0
for rec in records:
    rec_id, pm10, pm25 = rec
    aqi = calculate_aqi(pm10, pm25)
    cursor.execute("UPDATE dbo.Measurements SET AQI = ? WHERE Id = ?", aqi, rec_id)
    updated += 1

conn.commit()
print(f"✅ Zaktualizowano {updated} rekordów z wartościami AQI")

cursor.close()
conn.close()
print("🔒 Połączenie z bazą zamknięte.")
