#!/usr/bin/env python3
"""
Teste para verificar se as correções do CLI funcionaram
"""

import asyncio
import sys
import os

# Adicionar o diretório atual ao path para importar os módulos
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_connection_fix():
    """Testa se a correção do execute_async funcionou"""
    print("=== TESTE DE CONEXÃO ASSÍNCRONA ===")
    try:
        from caspyorm import connection
        
        async def test():
            await connection.connect_async(contact_points=['localhost'], keyspace='biblioteca')
            print("✅ Conexão assíncrona estabelecida")
            
            # Testar execute_async
            result = await connection.execute_async("SELECT release_version FROM system.local")
            print(f"✅ Query assíncrona executada: {result.one()}")
            
            await connection.disconnect_async()
            print("✅ Desconexão assíncrona realizada")
        
        asyncio.run(test())
        return True
    except Exception as e:
        print(f"❌ Erro na conexão assíncrona: {e}")
        return False

def test_uuid_conversion():
    """Testa se a conversão de UUID funciona"""
    print("\n=== TESTE DE CONVERSÃO UUID ===")
    try:
        from cli.main import parse_filters
        
        # Teste com UUID válido
        filters = ['id=123e4567-e89b-12d3-a456-426614174000']
        result = parse_filters(filters)
        print(f"✅ UUID convertido: {result['id']} (tipo: {type(result['id'])})")
        
        # Teste com UUID inválido (deve manter como string)
        filters = ['id=invalid-uuid']
        result = parse_filters(filters)
        print(f"✅ UUID inválido mantido como string: {result['id']} (tipo: {type(result['id'])})")
        
        # Teste com autor_id
        filters = ['autor_id=123e4567-e89b-12d3-a456-426614174000']
        result = parse_filters(filters)
        print(f"✅ autor_id convertido: {result['autor_id']} (tipo: {type(result['autor_id'])})")
        
        return True
    except Exception as e:
        print(f"❌ Erro na conversão UUID: {e}")
        return False

def test_query_async():
    """Testa se as queries assíncronas funcionam"""
    print("\n=== TESTE DE QUERIES ASSÍNCRONAS ===")
    try:
        from caspyorm import connection
        
        async def test():
            await connection.connect_async(contact_points=['localhost'], keyspace='biblioteca')
            print("✅ Conectado para teste de queries")
            
            # Testar execute_async com query simples
            result = await connection.execute_async("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'biblioteca'")
            rows = list(result)
            print(f"✅ Query assíncrona executada: {len(rows)} resultados")
            
            await connection.disconnect_async()
            print("✅ Desconectado")
        
        asyncio.run(test())
        return True
    except Exception as e:
        print(f"❌ Erro nas queries assíncronas: {e}")
        return False

def main():
    """Executa todos os testes"""
    print("🧪 Testando correções do CLI CaspyORM...")
    
    tests = [
        test_connection_fix,
        test_uuid_conversion,
        test_query_async
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"❌ Teste falhou com exceção: {e}")
    
    print(f"\n📊 Resultados: {passed}/{total} testes passaram")
    
    if passed == total:
        print("🎉 Todas as correções funcionaram!")
        return 0
    else:
        print("⚠️ Algumas correções ainda precisam de ajustes")
        return 1

if __name__ == "__main__":
    exit(main()) 