import os
import requests
import pyodbc

STATIONS = {
    "Warszawa": {"id": 52},
    "Kraków": {"id": 400},
    "Łódź": {"id": 70},
    "Wrocław": {"id": 114},
    "Poznań": {"id": 137},
    "Gdańsk": {"id": 225},
    "Szczecin": {"id": 609},
    "Bydgoszcz": {"id": 147},
    "Lublin": {"id": 176},
    "Katowice": {"id": 174},
    "Białystok": {"id": 101},
    "Gdynia": {"id": 228},
    "Częstochowa": {"id": 534},
    "Radom": {"id": 193},
    "Toruń": {"id": 179},
    "Kielce": {"id": 204},
    "Rzeszów": {"id": 203},
    "Gliwice": {"id": 606},
    "Zabrze": {"id": 618},
    "Olsztyn": {"id": 144}
}

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DATABASE')};"
    f"UID={os.getenv('SQL_USERNAME')};"
    f"PWD={os.getenv('SQL_PASSWORD')}"
)

def get_sensor_values(sensor_id):
    if not sensor_id:
        return {}
    response = requests.get(
        f"https://api.gios.gov.pl/pjp-api/rest/data/getData/{sensor_id}"
    ).json()
    return {v['date'][:10]: v['value'] for v in response.get("values", []) if v['value'] is not None}

def fetch_and_store():
    for name, st in STATIONS.items():
        sensors = requests.get(f"https://api.gios.gov.pl/pjp-api/rest/station/sensors/{st['id']}").json()
        pm10_id = next((s['id'] for s in sensors if s['param']['paramCode'] == 'PM10'), None)
        pm25_id = next((s['id'] for s in sensors if s['param']['paramCode'] == 'PM2.5'), None)

        pm10 = get_sensor_values(pm10_id)
        pm25 = get_sensor_values(pm25_id)
        dates = sorted(set(pm10) | set(pm25))

        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            for date in dates:
                if pm10.get(date) is None and pm25.get(date) is None:
                    continue
                cursor.execute("""
                    MERGE pomiar WITH (HOLDLOCK) AS tgt
                    USING (SELECT ? AS stacja, ? AS data) AS src
                    ON tgt.stacja = src.stacja AND tgt.data = src.data
                    WHEN NOT MATCHED THEN
                    INSERT (stacja, data, pm10, pm25)
                    VALUES (?, ?, ?, ?);
                """, name, date, name, date, pm10.get(date), pm25.get(date))
            conn.commit()
