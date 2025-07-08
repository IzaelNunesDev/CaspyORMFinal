#!/usr/bin/env python3
"""
Script para atualizar patches nos arquivos de teste após a reorganização.
"""

import os
import re
from pathlib import Path

def update_patches_in_file(file_path: str):
    """Atualiza patches em um arquivo específico."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Mapeamento de patches antigos para novos
    patch_mappings = {
        "patch('caspyorm.connection": "patch('caspyorm.core.connection",
        "patch('caspyorm.model": "patch('caspyorm.core.model",
        "patch('caspyorm.query": "patch('caspyorm.core.query",
        "patch('caspyorm.fields": "patch('caspyorm.core.fields",
        "patch('caspyorm._internal": "patch('caspyorm._internal",
        "patch('caspyorm.exceptions": "patch('caspyorm.utils.exceptions",
        "patch('caspyorm.logging": "patch('caspyorm.utils.logging",
        "patch('caspyorm.batch": "patch('caspyorm.types.batch",
        "patch('caspyorm.usertype": "patch('caspyorm.types.usertype",
    }
    
    # Aplicar substituições
    for old_patch, new_patch in patch_mappings.items():
        content = content.replace(old_patch, new_patch)
    
    # Escrever de volta
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"Atualizado: {file_path}")

def main():
    """Atualiza todos os arquivos de teste."""
    tests_dir = Path("tests")
    
    # Encontrar todos os arquivos Python em tests/
    for py_file in tests_dir.rglob("*.py"):
        if py_file.is_file():
            update_patches_in_file(str(py_file))

if __name__ == "__main__":
    main() 