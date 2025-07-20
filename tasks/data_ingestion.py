import requests
import os

def ingest_breweries_data(output_path: str):
    url = "https://api.openbrewerydb.org/v1/breweries"
    response = requests.get(url)
    response.raise_for_status()

    os.makedirs(output_path, exist_ok=True)

    file_name = "breweries.json"
    with open(os.path.join(output_path, file_name), "w") as f:
        f.write(response.text)