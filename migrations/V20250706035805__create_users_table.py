# Migration: create_users_table
# Created at: 2025-07-06 03:58:05.491147

import asyncio
from caspyorm import connection
from rich.console import Console

console = Console()

async def upgrade():
    """Aplica as mudanças desta migração."""
    console.print(f"[bold yellow]Aplicando migração: create_users_table[/bold yellow]")
    # Exemplo: await connection.execute_async("CREATE TABLE IF NOT EXISTS users (id uuid PRIMARY KEY, name text)")
    pass

async def downgrade():
    """Reverte as mudanças desta migração."""
    console.print(f"[bold yellow]Revertendo migração: create_users_table[/bold yellow]")
    # Exemplo: await connection.execute_async("DROP TABLE IF EXISTS users")
    pass
