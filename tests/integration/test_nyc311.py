import pytest
from tests.models import NYC311

@pytest.mark.usefixtures("db_connection")
def test_select_some_data():
    # Busca alguns registros reais
    results = NYC311.all().limit(5).all()
    assert len(results) > 0
    for row in results:
        assert row.unique_key
        assert row.complaint_type

@pytest.mark.usefixtures("db_connection")
def test_insert_and_delete():
    obj = NYC311(
        unique_key="test_key_123",
        created_date="2024-07-07 12:00:00",
        complaint_type="Test",
        descriptor="Test Desc",
        incident_address="Test Address"
    )
    obj.save()
    found = NYC311.get(unique_key="test_key_123")
    assert found is not None
    found.delete()
    assert NYC311.get(unique_key="test_key_123") is None 