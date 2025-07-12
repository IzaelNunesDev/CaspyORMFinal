#!/usr/bin/env python3
"""
Script de build para CaspyORM
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

def clean_build():
    """Limpa arquivos de build anteriores"""
    dirs_to_clean = ['build', 'dist', '*.egg-info']
    for pattern in dirs_to_clean:
        for path in Path('.').glob(pattern):
            if path.is_dir():
                shutil.rmtree(path)
                print(f"Removido: {path}")
            elif path.is_file():
                path.unlink()
                print(f"Removido: {path}")

def build_package():
    """Constrói o pacote"""
    print("Construindo pacote...")
    subprocess.run([sys.executable, "-m", "build"], check=True)
    print("Pacote construído com sucesso!")

def check_package():
    """Verifica o pacote construído"""
    print("Verificando pacote...")
    subprocess.run([sys.executable, "-m", "twine", "check", "dist/*"], check=True)
    print("Pacote verificado com sucesso!")

def upload_to_testpypi():
    """Faz upload para TestPyPI"""
    print("Fazendo upload para TestPyPI...")
    subprocess.run([sys.executable, "-m", "twine", "upload", "--repository", "testpypi", "dist/*"], check=True)
    print("Upload para TestPyPI concluído!")

def upload_to_pypi():
    """Faz upload para PyPI"""
    print("Fazendo upload para PyPI...")
    subprocess.run([sys.executable, "-m", "twine", "upload", "dist/*"], check=True)
    print("Upload para PyPI concluído!")

def main():
    if len(sys.argv) < 2:
        print("Uso: python build.py [clean|build|check|test-upload|upload]")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "clean":
        clean_build()
    elif command == "build":
        clean_build()
        build_package()
    elif command == "check":
        build_package()
        check_package()
    elif command == "test-upload":
        clean_build()
        build_package()
        check_package()
        upload_to_testpypi()
    elif command == "upload":
        clean_build()
        build_package()
        check_package()
        upload_to_pypi()
    else:
        print("Comando inválido. Use: clean, build, check, test-upload, ou upload")

if __name__ == "__main__":
    main() 