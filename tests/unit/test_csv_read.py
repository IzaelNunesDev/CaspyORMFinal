import pandas as pd
import os
import pytest

CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "nyc_311.csv")

REQUIRED_COLUMNS = [
    "unique_key", "created_date", "complaint_type", "descriptor", "incident_address"
]

def test_csv_columns_exist():
    df = pd.read_csv(CSV_PATH, nrows=1)
    for col in REQUIRED_COLUMNS:
        assert col in df.columns, f"Coluna obrigatória ausente: {col}"

def test_csv_read_some_rows():
    df = pd.read_csv(CSV_PATH, usecols=REQUIRED_COLUMNS, nrows=10)
    assert len(df) > 0
    assert all(col in df.columns for col in REQUIRED_COLUMNS)
    # Verifica se há pelo menos um valor não nulo em cada coluna obrigatória
    for col in REQUIRED_COLUMNS:
        assert df[col].notnull().any(), f"Coluna {col} está totalmente vazia nas primeiras linhas" 