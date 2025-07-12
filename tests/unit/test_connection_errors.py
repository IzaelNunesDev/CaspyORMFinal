import pytest
from caspyorm.core.connection import connect, disconnect
from caspyorm.utils.exceptions import ConnectionError

def test_connection_error_invalid_host():
    with pytest.raises(ConnectionError):
        connect(contact_points=["host_inexistente_12345"], keyspace="nyc_data", port=9042)
    # Não precisa desconectar pois a conexão não foi estabelecida 