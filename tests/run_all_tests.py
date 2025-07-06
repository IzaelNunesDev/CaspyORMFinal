#!/usr/bin/env python3
"""
Script para executar todos os testes do CaspyORM de forma organizada.
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path

# Detecta o Python do venv ou usa sys.executable
VENV_PYTHON = os.path.join(os.path.dirname(__file__), '..', 'venv', 'bin', 'python')
PYTHON_EXEC = VENV_PYTHON if os.path.exists(VENV_PYTHON) else sys.executable

def run_tests(test_path, coverage=False, verbose=False, markers=None):
    """Executa testes em um caminho específico."""
    cmd = [PYTHON_EXEC, "-m", "pytest", test_path]
    
    if coverage:
        cmd.extend([
            "--cov=caspyorm",
            "--cov-report=term-missing",
            "--cov-report=html",
            "--cov-report=xml"
        ])
    
    if verbose:
        cmd.append("-v")
    
    if markers:
        cmd.extend(["-m", markers])
    
    print(f"Executando testes em: {test_path}")
    print(f"Comando: {' '.join(cmd)}")
    print("-" * 50)
    
    result = subprocess.run(cmd, capture_output=False)
    return result.returncode


def run_unit_tests(coverage=False, verbose=False):
    """Executa testes unitários."""
    print("🧪 EXECUTANDO TESTES UNITÁRIOS")
    print("=" * 50)
    
    unit_tests = [
        "tests/unit/test_fields.py",
        "tests/unit/test_connection.py", 
        "tests/unit/test_model.py",
        "tests/unit/test_query.py",
        "tests/unit/test_internal_model_construction.py",
        "tests/unit/test_internal_serialization.py",
        "tests/unit/test_exceptions.py",
        "tests/unit/test_logging.py"
    ]
    
    failed = 0
    for test_file in unit_tests:
        if os.path.exists(test_file):
            result = run_tests(test_file, coverage, verbose)
            if result != 0:
                failed += 1
        else:
            print(f"⚠️  Arquivo não encontrado: {test_file}")
    
    return failed


def run_integration_tests(coverage=False, verbose=False):
    """Executa testes de integração."""
    print("\n🔗 EXECUTANDO TESTES DE INTEGRAÇÃO")
    print("=" * 50)
    
    integration_tests = [
        "tests/integration/test_model_integration.py"
    ]
    
    failed = 0
    for test_file in integration_tests:
        if os.path.exists(test_file):
            result = run_tests(test_file, coverage, verbose)
            if result != 0:
                failed += 1
        else:
            print(f"⚠️  Arquivo não encontrado: {test_file}")
    
    return failed


def run_cli_tests(coverage=False, verbose=False):
    """Executa testes do CLI."""
    print("\n💻 EXECUTANDO TESTES DO CLI")
    print("=" * 50)
    
    cli_tests = [
        "tests/cli/test_cli.py",
        "tests/cli/test_cli_commands.py",
        "tests/cli/test_cli_basic.py"
    ]
    
    failed = 0
    for test_file in cli_tests:
        if os.path.exists(test_file):
            result = run_tests(test_file, coverage, verbose)
            if result != 0:
                failed += 1
        else:
            print(f"⚠️  Arquivo não encontrado: {test_file}")
    
    return failed


def run_specific_test_category(category, coverage=False, verbose=False):
    """Executa testes de uma categoria específica."""
    if category == "unit":
        return run_unit_tests(coverage, verbose)
    elif category == "integration":
        return run_integration_tests(coverage, verbose)
    elif category == "cli":
        return run_cli_tests(coverage, verbose)
    else:
        print(f"❌ Categoria desconhecida: {category}")
        return 1


def run_all_tests(coverage=False, verbose=False):
    """Executa todos os testes."""
    print("🚀 EXECUTANDO TODOS OS TESTES DO CASPYORM")
    print("=" * 60)
    
    total_failed = 0
    
    # Testes unitários
    failed = run_unit_tests(coverage, verbose)
    total_failed += failed
    
    # Testes de integração
    failed = run_integration_tests(coverage, verbose)
    total_failed += failed
    
    # Testes do CLI
    failed = run_cli_tests(coverage, verbose)
    total_failed += failed
    
    return total_failed


def run_tests_with_markers(markers, coverage=False, verbose=False):
    """Executa testes com marcadores específicos."""
    print(f"🏷️  EXECUTANDO TESTES COM MARCADORES: {markers}")
    print("=" * 50)
    
    result = run_tests("tests/", coverage, verbose, markers)
    return result


def generate_test_report():
    """Gera relatório de testes."""
    print("📊 GERANDO RELATÓRIO DE TESTES")
    print("=" * 50)
    
    cmd = [
        PYTHON_EXEC, "-m", "pytest", 
        "tests/",
        "--cov=caspyorm",
        "--cov-report=html:htmlcov",
        "--cov-report=xml:coverage.xml",
        "--cov-report=term-missing",
        "--junitxml=test-results.xml"
    ]
    
    result = subprocess.run(cmd, capture_output=False)
    
    if result.returncode == 0:
        print("✅ Relatório gerado com sucesso!")
        print("📁 Arquivos gerados:")
        print("   - htmlcov/ (relatório HTML)")
        print("   - coverage.xml (relatório XML)")
        print("   - test-results.xml (resultados JUnit)")
    else:
        print("❌ Erro ao gerar relatório")
    
    return result.returncode


def check_test_environment():
    """Verifica se o ambiente de teste está configurado."""
    print("🔍 VERIFICANDO AMBIENTE DE TESTE")
    print("=" * 50)
    
    # Verifica se pytest está instalado
    try:
        import pytest
        print(f"✅ pytest {pytest.__version__} instalado")
    except ImportError:
        print("❌ pytest não está instalado")
        return False
    
    # Verifica se pytest-cov está instalado
    try:
        import pytest_cov
        print(f"✅ pytest-cov instalado")
    except ImportError:
        print("⚠️  pytest-cov não está instalado (cobertura não disponível)")
    
    # Verifica se pytest-asyncio está instalado
    try:
        import pytest_asyncio
        print(f"✅ pytest-asyncio instalado")
    except ImportError:
        print("⚠️  pytest-asyncio não está instalado (testes async podem falhar)")
    
    # Verifica estrutura de diretórios
    test_dirs = ["tests/unit", "tests/integration", "tests/cli", "tests/fixtures"]
    for test_dir in test_dirs:
        if os.path.exists(test_dir):
            print(f"✅ {test_dir} existe")
        else:
            print(f"⚠️  {test_dir} não existe")
    
    return True


def main():
    """Função principal."""
    parser = argparse.ArgumentParser(description="Executa testes do CaspyORM")
    parser.add_argument(
        "--category", "-c",
        choices=["unit", "integration", "cli", "all"],
        default="all",
        help="Categoria de testes a executar"
    )
    parser.add_argument(
        "--coverage", "-C",
        action="store_true",
        help="Executa com cobertura de código"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Executa em modo verboso"
    )
    parser.add_argument(
        "--markers", "-m",
        help="Executa apenas testes com marcadores específicos"
    )
    parser.add_argument(
        "--report", "-r",
        action="store_true",
        help="Gera relatório de testes"
    )
    parser.add_argument(
        "--check", "-k",
        action="store_true",
        help="Verifica ambiente de teste"
    )
    
    args = parser.parse_args()
    
    # Verifica ambiente se solicitado
    if args.check:
        if not check_test_environment():
            sys.exit(1)
        return
    
    # Gera relatório se solicitado
    if args.report:
        result = generate_test_report()
        sys.exit(result)
    
    # Executa testes com marcadores se especificado
    if args.markers:
        result = run_tests_with_markers(args.markers, args.coverage, args.verbose)
        sys.exit(result)
    
    # Executa testes por categoria
    if args.category == "all":
        result = run_all_tests(args.coverage, args.verbose)
    else:
        result = run_specific_test_category(args.category, args.coverage, args.verbose)
    
    # Resumo final
    print("\n" + "=" * 60)
    if result == 0:
        print("🎉 TODOS OS TESTES PASSARAM!")
    else:
        print(f"❌ {result} CATEGORIA(S) DE TESTE FALHARAM")
    
    sys.exit(result)


if __name__ == "__main__":
    main() 