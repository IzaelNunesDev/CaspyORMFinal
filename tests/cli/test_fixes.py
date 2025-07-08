import asyncio
import os
import sys
import pytest

# Adicionar o diretório raiz do projeto ao sys.path para que os módulos possam ser encontrados
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.caspyorm import connection
from cli.main import parse_filters

@pytest.mark.asyncio
async def test_connection_fix():
    """Testa se a correção do execute_async funcionou"""
    print("=== TESTE DE CONEXÃO ASSÍNCRONA ===")
    try:
        await connection.connect_async(contact_points=['localhost'], keyspace='biblioteca')
        print("✅ Conexão assíncrona estabelecida")
        
        # Testar execute_async
        result = await connection.execute_async("SELECT release_version FROM system.local")
        assert result.one() is not None
        print(f"✅ Query assíncrona executada: {result.one()}")
        
        await connection.disconnect_async()
        print("✅ Desconexão assíncrona realizada")
    except Exception as e:
        pytest.fail(f"❌ Erro na conexão assíncrona: {e}")

def test_uuid_conversion():
    """Testa se a conversão de UUID funciona"""
    print("\n=== TESTE DE CONVERSÃO UUID ===")
    import uuid
    
    # Teste com UUID válido
    filters = ['id=123e4567-e89b-12d3-a456-426614174000']
    result = parse_filters(filters)
    assert isinstance(result['id'], uuid.UUID)
    assert str(result['id']) == '123e4567-e89b-12d3-a456-426614174000'
    print(f"✅ UUID convertido: {result['id']} (tipo: {type(result['id'])})")
    
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
    try:
        await connection.connect_async(contact_points=['localhost'], keyspace='biblioteca')
        print("✅ Conectado para teste de queries")
        
        # Testar execute_async com query simples
        result = await connection.execute_async("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'biblioteca'")
        rows = list(result)
        assert len(rows) > 0 # Assuming 'biblioteca' keyspace exists for this test
        print(f"✅ Query assíncrona executada: {len(rows)} resultados")
        
        await connection.disconnect_async()
        print("✅ Desconectado")
    except Exception as e:
        pytest.fail(f"❌ Erro nas queries assíncronas: {e}")
