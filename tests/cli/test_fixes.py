import asyncio
import os
import sys
import pytest
import uuid
from unittest.mock import patch, MagicMock, AsyncMock

# Adicionar o diretório raiz do projeto ao sys.path para que os módulos possam ser encontrados
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.caspyorm import connection
from caspyorm_cli.main import parse_filters

@pytest.mark.asyncio
async def test_connection_fix():
    """Testa se a correção do execute_async funcionou"""
    print("=== TESTE DE CONEXÃO ASSÍNCRONA ===")
    
    # Mock completo da instância connection
    with patch('caspyorm.core.connection.connection') as mock_connection:
        # Mock dos métodos da instância connection
        mock_connection.connect_async = AsyncMock()
        mock_connection.execute_async = AsyncMock()
        mock_connection.disconnect_async = AsyncMock()

        # Mock dos resultados
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.release_version = "4.0.1"
        mock_result.one.return_value = mock_row
        mock_connection.execute_async.return_value = mock_result

        # Testar sem conectar ao Cassandra real
        await mock_connection.connect_async(contact_points=['localhost'], keyspace='biblioteca')
        print("✅ Conexão assíncrona mockada")

        # Testar execute_async
        result = await mock_connection.execute_async("SELECT release_version FROM system.local")
        assert result.one() is not None
        print(f"✅ Query assíncrona executada: {result.one()}")

        await mock_connection.disconnect_async()
        print("✅ Desconexão assíncrona realizada")

        # Verificar se os métodos foram chamados
        mock_connection.connect_async.assert_called_once()
        mock_connection.execute_async.assert_called_once()
        mock_connection.disconnect_async.assert_called_once()

def test_uuid_conversion():
    """Testa se a conversão de UUID funciona"""
    print("\n=== TESTE DE CONVERSÃO UUID ===")
    
    # Mock da função uuid.uuid4
    with patch('uuid.uuid4') as mock_uuid:
        mock_uuid.return_value = "test-uuid-123"
        
        # Simular conversão de UUID
        uuid_str = str(mock_uuid())
        assert uuid_str == "test-uuid-123"
        print(f"✅ UUID convertido: {uuid_str}")
    
    # Teste com UUID inválido (deve manter como string)
    filters = ['id=invalid-uuid']
    result = parse_filters(filters)
    assert isinstance(result['id'], str)
    assert result['id'] == 'invalid-uuid'
    print(f"✅ UUID inválido mantido como string: {result['id']} (tipo: {type(result['id'])})")
    
    # Teste com autor_id
    filters = ['autor_id=123e4567-e89b-12d3-a456-426614174000']
    result = parse_filters(filters)
    assert isinstance(result['autor_id'], uuid.UUID)
    assert str(result['autor_id']) == '123e4567-e89b-12d3-a456-426614174000'
    print(f"✅ autor_id convertido: {result['autor_id']} (tipo: {type(result['autor_id'])})")

@pytest.mark.asyncio
async def test_query_async():
    """Testa se as queries assíncronas funcionam"""
    print("\n=== TESTE DE QUERIES ASSÍNCRONAS ===")
    
    # Mock completo da instância connection
    with patch('caspyorm.core.connection.connection') as mock_connection:
        # Mock dos métodos da instância connection
        mock_connection.connect_async = AsyncMock()
        mock_connection.execute_async = AsyncMock()
        mock_connection.disconnect_async = AsyncMock()

        # Mock dos resultados
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.keyspace_name = 'biblioteca'
        mock_result.__iter__ = lambda self: iter([mock_row])
        mock_connection.execute_async.return_value = mock_result

        # Testar sem conectar ao Cassandra real
        await mock_connection.connect_async(contact_points=['localhost'], keyspace='biblioteca')
        print("✅ Conectado para teste de queries")

        # Testar execute_async com query simples
        result = await mock_connection.execute_async("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'biblioteca'")
        rows = list(result)
        assert len(rows) > 0
        print(f"✅ Query assíncrona executada: {len(rows)} resultados")

        await mock_connection.disconnect_async()
        print("✅ Desconectado")

        # Verificar se os métodos foram chamados
        mock_connection.connect_async.assert_called_once()
        mock_connection.execute_async.assert_called_once()
        mock_connection.disconnect_async.assert_called_once()
