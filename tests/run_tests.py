#!/usr/bin/env python3
"""
Script para executar testes do CLI do CaspyORM.
"""

import sys
import os
import subprocess

def run_test_file(test_file):
    """Executa um arquivo de teste específico."""
    print(f"\n🧪 Executando {test_file}...")
    try:
        result = subprocess.run([sys.executable, test_file], 
                              capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        if result.returncode == 0:
            print(f"✅ {test_file} - PASSOU")
            if result.stdout:
                print(result.stdout.strip())
        else:
            print(f"❌ {test_file} - FALHOU")
            if result.stderr:
                print(result.stderr.strip())
            return False
    except Exception as e:
        print(f"❌ Erro ao executar {test_file}: {e}")
        return False
    
    return True

def main():
    """Executa todos os testes."""
    print("🚀 Iniciando testes do CLI do CaspyORM...")
    
    # Lista de arquivos de teste
    test_files = [
        "test_cli_basic.py",
        "test_cli_commands.py"
    ]
    
    passed = 0
    total = len(test_files)
    
    for test_file in test_files:
        if run_test_file(test_file):
            passed += 1
    
    print(f"\n📊 Resultado: {passed}/{total} testes passaram")
    
    if passed == total:
        print("🎉 Todos os testes passaram!")
        return 0
    else:
        print("⚠️  Alguns testes falharam!")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 