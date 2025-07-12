#!/usr/bin/env python3
import time
from sodapy import Socrata
import pandas as pd
import os

APP_TOKEN = os.environ.get("NYC_APP_TOKEN")
DOMAIN = "data.cityofnewyork.us"
DATASET_ID = os.environ.get("NYC_DATASET", "fhrw-4uyv")
LIMIT = int(os.environ.get("NYC_LIMIT", 5000))
MAX_RECORDS = int(os.environ.get("NYC_MAX", 10000))
OUTPUT_PATH = os.environ.get(
    "NYC_OUTPUT",
    os.path.join(os.path.dirname(__file__), "..", "tests", "data", "nyc_311.csv"),
)


def fetch_data():
    client = Socrata(DOMAIN, APP_TOKEN, timeout=120)
    rows = []
    offset = 0
    retries = 3

    while offset < MAX_RECORDS:
        fetch = min(LIMIT, MAX_RECORDS - offset)
        for attempt in range(retries):
            try:
                print(f"Buscando registros {offset}–{offset + fetch} (tentativa {attempt+1})...")
                batch = client.get(DATASET_ID, limit=fetch, offset=offset)
                break
            except Exception as e:
                print(f"Erro: {e}. Retentando em {2**attempt} segundos...")
                time.sleep(2 ** attempt)
        else:
            print("Falha após múltiplas tentativas. Abortando.")
            break
        if not batch:
            break
        rows.extend(batch)
        offset += len(batch)
        if len(batch) < fetch:
            break

    return pd.DataFrame.from_records(rows)

def main():
    df = fetch_data()
    print(f"Total de registros coletados: {len(df)}")
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Dados salvos em {OUTPUT_PATH}")

if __name__ == "__main__":
    main() 