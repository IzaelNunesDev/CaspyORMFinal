from caspyorm.core.fields import Text
from caspyorm.core.model import Model

class NYC311(Model):
    __table_name__ = "nyc_311"
    unique_key = Text(primary_key=True, required=True)
    created_date = Text()
    complaint_type = Text()
    descriptor = Text()
    incident_address = Text() 