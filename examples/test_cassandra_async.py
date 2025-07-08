#!/usr/bin/env python3
"""
Teste para entender como execute_async funciona no driver do Cassandra
"""

import asyncio
from cassandra.cluster import Cluster

def test_sync():
    """Testa execução síncrona"""
    print("=== TESTE SÍNCRONO ===")
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        
        # Execução síncrona
        result = session.execute("SELECT release_version FROM system.local")
        print(f"Resultado síncrono: {result.one()}")
        
        session.shutdown()
        cluster.shutdown()
    except Exception as e:
        print(f"Erro síncrono: {e}")

def test_async():
    """Testa execução assíncrona"""
    print("=== TESTE ASSÍNCRONO ===")
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        
        # Execução assíncrona - retorna ResponseFuture
        future = session.execute_async("SELECT release_version FROM system.local")
        print(f"Tipo do future: {type(future)}")
        
        # Para obter o resultado, precisamos chamar .result()
        result = future.result()
        print(f"Resultado assíncrono: {result.one()}")
        
        session.shutdown()
        cluster.shutdown()
    except Exception as e:
        print(f"Erro assíncrono: {e}")

async def test_async_with_await():
    """Testa execução assíncrona com await"""
    print("=== TESTE ASSÍNCRONO COM AWAIT ===")
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        
        # Execução assíncrona - retorna ResponseFuture
        future = session.execute_async("SELECT release_version FROM system.local")
        print(f"Tipo do future: {type(future)}")
        
        # Para usar com await, precisamos usar asyncio.to_thread
        result = await asyncio.to_thread(future.result)
        print(f"Resultado com await: {result.one()}")
        
        session.shutdown()
        cluster.shutdown()
    except Exception as e:
        print(f"Erro assíncrono com await: {e}")

if __name__ == "__main__":
    print("Testando diferentes formas de execução do Cassandra...")
    
    # Teste síncrono
    test_sync()
    
    # Teste assíncrono
    test_async()
    
    # Teste assíncrono com await
    asyncio.run(test_async_with_await()) 