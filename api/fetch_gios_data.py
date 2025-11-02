import requests
import json

# Nowy endpoint GIOŚ dla listy stacji pomiarowych
url = "https://api.gios.gov.pl/pjp-api/v1/rest/station/findAll"

try:
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    # zapisz dane
    with open("stations.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Pobrano {len(data)} stacji i zapisano do pliku stations.json ✅")

except requests.exceptions.RequestException as e:
    print("Błąd podczas pobierania danych z API GIOŚ:", e)
