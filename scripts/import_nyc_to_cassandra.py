import os
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

CSV_PATH = os.environ.get(
    "NYC_CSV",
    os.path.join(os.path.dirname(__file__), "..", "tests", "data", "nyc_311.csv"),
)
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra_nyc")
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "nyc_data")
TABLE = os.environ.get("CASSANDRA_TABLE", "nyc_311")

# Defina as colunas principais do CSV que serão usadas na tabela
COLUMNS = [
    "unique_key", "created_date", "complaint_type", "descriptor", "incident_address"
]

CREATE_KEYSPACE = f"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
"""

CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (
    unique_key text PRIMARY KEY,
    created_date text,
    complaint_type text,
    descriptor text,
    incident_address text
);
"""

INSERT_QUERY = f"""
INSERT INTO {KEYSPACE}.{TABLE} (unique_key, created_date, complaint_type, descriptor, incident_address)
VALUES (?, ?, ?, ?, ?)
"""

def main():
    print(f"Lendo CSV: {CSV_PATH}")
    # Descobrir colunas disponíveis e filtrar apenas as que existem
    available_cols = pd.read_csv(CSV_PATH, nrows=1).columns.tolist()
    use_cols = tuple(col for col in COLUMNS if col in available_cols)
    df = pd.read_csv(CSV_PATH, usecols=use_cols)  # type: ignore
    print(f"Total de registros a importar: {len(df)}")

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()
    session.execute(CREATE_KEYSPACE)
    session.set_keyspace(KEYSPACE)
    session.execute(CREATE_TABLE)

    prepared = session.prepare(INSERT_QUERY)
    for i, row in df.iterrows():
        values = [str(row.get(col, '')) for col in use_cols]
        session.execute(prepared, values)
        if (i+1) % 1000 == 0:
            print(f"{i+1} registros inseridos...")
    print("Importação concluída!")
    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main() 