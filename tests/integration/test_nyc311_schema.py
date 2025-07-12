import pytest
from caspyorm.core.connection import execute, connect
from tests.models import NYC311
from caspyorm.utils.exceptions import ValidationError


def test_table_exists(db_connection):
    connect(contact_points=["172.18.0.2"], keyspace="nyc_data", port=9042)
    result = execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='nyc_data' AND table_name='nyc_311'")
    assert any(row.table_name == "nyc_311" for row in result), "Tabela nyc_311 não existe!"


def test_schema_fields(db_connection):
    connect(contact_points=["172.18.0.2"], keyspace="nyc_data", port=9042)
    result = execute("SELECT column_name FROM system_schema.columns WHERE keyspace_name='nyc_data' AND table_name='nyc_311'")
    columns = {row.column_name for row in result}
    required = {"unique_key", "created_date", "complaint_type", "descriptor", "incident_address"}
    assert required.issubset(columns), f"Faltam colunas: {required - columns}"


def test_insert_invalid_missing_field(db_connection):
    connect(contact_points=["172.18.0.2"], keyspace="nyc_data", port=9042)
    with pytest.raises(ValidationError):
        NYC311(
            created_date="2024-07-07 12:00:00",
            complaint_type="Noise",
            descriptor="Loud music",
            incident_address="123 Main St"
        ).save()


def test_insert_invalid_type(db_connection):
    connect(contact_points=["172.18.0.2"], keyspace="nyc_data", port=9042)
    with pytest.raises(ValidationError):
        NYC311(
            unique_key=12345,  # deveria ser str
            created_date="2024-07-07 12:00:00",
            complaint_type="Noise",
            descriptor="Loud music",
            incident_address="123 Main St"
        ).save()


def test_batch_insert(db_connection):
    connect(contact_points=["172.18.0.2"], keyspace="nyc_data", port=9042)
    objs = [
        NYC311(
            unique_key=f"batch_key_{i}",
            created_date="2024-07-07 12:00:00",
            complaint_type="BatchTest",
            descriptor="Batch Desc",
            incident_address="Batch Address"
        ) for i in range(5)
    ]
    for obj in objs:
        obj.save()
    # Verifica se todos foram inseridos
    for i in range(5):
        found = NYC311.get(unique_key=f"batch_key_{i}")
        assert found is not None
        found.delete() 