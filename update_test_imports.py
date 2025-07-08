#!/usr/bin/env python3
"""
Script para atualizar imports e patches nos arquivos de teste para usar o namespace src.caspyorm.
"""
import os
from pathlib import Path
import re

def update_imports_in_file(file_path: str):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Atualizar imports do tipo 'from caspyorm...' para 'from src.caspyorm...'
    content = re.sub(r'from (caspyorm[\.\w]*)', r'from src.\1', content)
    content = re.sub(r'import (caspyorm[\.\w]*)', r'import src.\1', content)
    # Atualizar patches do tipo patch('caspyorm...' para patch('src.caspyorm...'
    content = re.sub(r"patch\('caspyorm([\.\w]*)", r"patch('src.caspyorm\1", content)
    # Atualizar patches do tipo patch("caspyorm..." para patch("src.caspyorm..."
    content = re.sub(r'patch\("caspyorm([\.\w]*)', r'patch("src.caspyorm\1', content)
    # Corrigir imports errados de usertype
    content = content.replace('from src.caspyorm.core.usertype', 'from src.caspyorm.types.usertype')
    content = content.replace('import src.caspyorm.core.usertype', 'import src.caspyorm.types.usertype')

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"Atualizado: {file_path}")

def main():
    tests_dir = Path("tests")
    for py_file in tests_dir.rglob("*.py"):
        if py_file.is_file():
            update_imports_in_file(str(py_file))

if __name__ == "__main__":
    main() 