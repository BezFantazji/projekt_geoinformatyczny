
from flask import Flask, render_template, request, make_response
import os, pyodbc, requests
from datetime import datetime, timedelta
from io import BytesIO

from apscheduler.schedulers.background import BackgroundScheduler
from azure.storage.blob import BlobServiceClient
from reportlab.lib.pagesizes import A4
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib import colors
#
app = Flask(__name__)

STATIONS = {
    "Warszawa": {"id": 52, "lat": 52.2298, "lon": 21.0118},
    "Kraków": {"id": 400, "lat": 50.0647, "lon": 19.9450},
    "Łódź": {"id": 70, "lat": 51.7592, "lon": 19.4550},
    "Wrocław": {"id": 114, "lat": 51.1079, "lon": 17.0385},
    "Poznań": {"id": 137, "lat": 52.4064, "lon": 16.9252},
    "Gdańsk": {"id": 225, "lat": 54.3520, "lon": 18.6466},
    "Szczecin": {"id": 609, "lat": 53.4285, "lon": 14.5528},
    "Bydgoszcz": {"id": 147, "lat": 53.1235, "lon": 18.0084},
    "Lublin": {"id": 176, "lat": 51.2465, "lon": 22.5684},
    "Katowice": {"id": 174, "lat": 50.2649, "lon": 19.0238},
    "Białystok": {"id": 101, "lat": 53.1325, "lon": 23.1688},
    "Gdynia": {"id": 228, "lat": 54.5189, "lon": 18.5305},
    "Częstochowa": {"id": 534, "lat": 50.8118, "lon": 19.1203},
    "Radom": {"id": 193, "lat": 51.4027, "lon": 21.1471},
    "Toruń": {"id": 179, "lat": 53.0138, "lon": 18.5984},
    "Kielce": {"id": 204, "lat": 50.8661, "lon": 20.6286},
    "Rzeszów": {"id": 203, "lat": 50.0413, "lon": 21.9990},
    "Gliwice": {"id": 606, "lat": 50.2945, "lon": 18.6714},
    "Zabrze": {"id": 618, "lat": 50.3249, "lon": 18.7857},
    "Olsztyn": {"id": 144, "lat": 53.7784, "lon": 20.4801}
}
SQL_SERVER   = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
    f"UID={SQL_USERNAME};PWD={SQL_PASSWORD}"
)

with pyodbc.connect(CONN_STR) as conn:
    cur = conn.cursor()
    cur.execute("""
        IF OBJECT_ID('pomiar', 'U') IS NULL
        CREATE TABLE pomiar (
            id INT IDENTITY PRIMARY KEY,
            stacja NVARCHAR(50),
            data DATE,
            pm10 FLOAT,
            pm25 FLOAT
        )
    """)
    conn.commit()

from fetch_from_api_to_sql import fetch_and_store  # noqa: E402

@app.route("/")
def index():
    city = request.args.get("station", "Warszawa")
    info = STATIONS.get(city)
    if not info:
        return "Stacja nieznaleziona", 404

    idx = requests.get(f"https://api.gios.gov.pl/pjp-api/rest/aqindex/getIndex/{info['id']}").json()
    with pyodbc.connect(CONN_STR) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT TOP 30 data, pm10, pm25
            FROM pomiar
            WHERE stacja = ? AND pm10 IS NOT NULL AND pm25 IS NOT NULL
            ORDER BY data DESC
        """, city)
        rows = cur.fetchall()[::-1]

    if rows:
        labels = [row[0].strftime("%Y-%m-%d") for row in rows]
        pm10   = [row[1] for row in rows]
        pm25   = [row[2] for row in rows]
    else:
        labels = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(29, -1, -1)]
        pm10 = pm25 = [0] * 30

    return render_template(
        "index.html",
        station=city,
        stations=sorted(STATIONS.keys()),
        index=idx,
        lat=info["lat"],
        lon=info["lon"],
        STATIONS=STATIONS,
        chart_data={"labels": labels, "pm10": pm10, "pm25": pm25}
    )

@app.route("/generuj-raport", methods=["POST"])
def generuj_raport():
    city = request.form.get("station", "Warszawa")
    info = STATIONS.get(city)
    if not info:
        return "Stacja nieznaleziona", 404

    idx = requests.get(f"https://api.gios.gov.pl/pjp-api/rest/aqindex/getIndex/{info['id']}").json()

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    title = ParagraphStyle(name="T", parent=styles["Heading1"], alignment=TA_CENTER, textColor=colors.HexColor("#0077cc"))
    body  = ParagraphStyle(name="B", parent=styles["BodyText"], spaceAfter=5)

    elems = [Paragraph(f"Raport jakości powietrza – {city}", title), Spacer(1, 12)]
    for k, v in idx.items():
        if isinstance(v, dict):
            v = v.get("indexLevelName", "brak danych")
        elems.append(Paragraph(f"<b>{k}:</b> {v}", body))
    doc.build(elems)

    try:
        blob = BlobServiceClient.from_connection_string(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))
        container = blob.get_container_client("reports")
        container.upload_blob(f"raport_{city}.pdf", buffer.getvalue(), overwrite=True)
    except Exception as e:
        return f"Błąd zapisu do Storage: {e}", 500

    resp = make_response(buffer.getvalue())
    resp.headers.update({"Content-Type": "application/pdf",
                         "Content-Disposition": f"inline; filename=raport_{city}.pdf"})
    return resp

if __name__ == "__main__":
    fetch_and_store()  # pierwszy import przy starcie
    sched = BackgroundScheduler()
    sched.add_job(fetch_and_store, "interval", hours=1)
    sched.start()

    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)

