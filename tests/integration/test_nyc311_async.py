import pytest

@pytest.mark.skip(reason="Suporte assíncrono desativado devido à incompatibilidade do driver com Cassandra 4.x")
def test_select_some_data_async():
    pass

@pytest.mark.skip(reason="Suporte assíncrono desativado devido à incompatibilidade do driver com Cassandra 4.x")
def test_insert_and_delete_async():
    pass 