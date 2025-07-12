import pytest
from tests.models import NYC311
from caspyorm.utils.exceptions import ValidationError

def test_model_dump():
    obj = NYC311(
        unique_key="unit_key_1",
        created_date="2024-07-07 12:00:00",
        complaint_type="Noise",
        descriptor="Loud music",
        incident_address="123 Main St"
    )
    data = obj.model_dump()
    assert data["unique_key"] == "unit_key_1"
    assert data["incident_address"] == "123 Main St"

def test_missing_required_field():
    with pytest.raises(ValidationError):
        NYC311(
            # unique_key ausente (obrigatório)
            created_date="2024-07-07 12:00:00",
            complaint_type="Noise",
            descriptor="Loud music",
            incident_address="123 Main St"
        )

def test_serialization_json():
    obj = NYC311(
        unique_key="unit_key_2",
        created_date="2024-07-07 12:00:00",
        complaint_type="Water Leak",
        descriptor="Pipe burst",
        incident_address="456 Side St"
    )
    json_str = obj.model_dump_json()
    assert "unit_key_2" in json_str
    assert "Pipe burst" in json_str 