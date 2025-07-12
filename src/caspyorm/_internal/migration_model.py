from ..core.fields import Text, Timestamp
from ..core.model import Model


class Migration(Model):
    __table_name__ = "caspyorm_migrations"
    version = Text(primary_key=True)  # Ex: "V20250706035805__create_users_table.py"
    applied_at = Timestamp(required=True)
