# Migration: nyc_restaurants_data
# Created at: 2025-07-06 07:25:27.123456

import asyncio
from caspyorm import connection
from rich.console import Console
import uuid
from datetime import datetime

console = Console()

async def upgrade():
    """Aplica as mudanças desta migração."""
    console.print(f"[bold yellow]Aplicando migração: nyc_restaurants_data[/bold yellow]")
    
    # Criar tabela de restaurantes de NYC
    create_table_query = """
    CREATE TABLE IF NOT EXISTS nyc_restaurants (
        id uuid PRIMARY KEY,
        name text,
        cuisine text,
        borough text,
        address text,
        phone text,
        rating decimal,
        price_range text,
        latitude decimal,
        longitude decimal,
        created_at timestamp,
        updated_at timestamp
    )
    """
    
    await connection.execute_async(create_table_query)
    console.print("[green]✅ Tabela nyc_restaurants criada com sucesso![/green]")
    
    # Inserir dados reais de restaurantes de NYC
    restaurants_data = [
        {
            "id": uuid.uuid4(),
            "name": "Katz's Delicatessen",
            "cuisine": "Jewish Deli",
            "borough": "Manhattan",
            "address": "205 E Houston St, New York, NY 10002",
            "phone": "(212) 254-2246",
            "rating": 4.5,
            "price_range": "$$",
            "latitude": 40.7223,
            "longitude": -73.9874,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        },
        {
            "id": uuid.uuid4(),
            "name": "Joe's Pizza",
            "cuisine": "Pizza",
            "borough": "Manhattan",
            "address": "123 Carmine St, New York, NY 10014",
            "phone": "(212) 366-1182",
            "rating": 4.3,
            "price_range": "$",
            "latitude": 40.7308,
            "longitude": -74.0027,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        },
        {
            "id": uuid.uuid4(),
            "name": "Peter Luger Steak House",
            "cuisine": "Steakhouse",
            "borough": "Brooklyn",
            "address": "178 Broadway, Brooklyn, NY 11211",
            "phone": "(718) 387-7400",
            "rating": 4.7,
            "price_range": "$$$",
            "latitude": 40.7097,
            "longitude": -73.9626,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        },
        {
            "id": uuid.uuid4(),
            "name": "Di Fara Pizza",
            "cuisine": "Pizza",
            "borough": "Brooklyn",
            "address": "1424 Ave J, Brooklyn, NY 11230",
            "phone": "(718) 258-1367",
            "rating": 4.6,
            "price_range": "$$",
            "latitude": 40.6189,
            "longitude": -73.9597,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        },
        {
            "id": uuid.uuid4(),
            "name": "Arthur Avenue Retail Market",
            "cuisine": "Italian",
            "borough": "Bronx",
            "address": "2344 Arthur Ave, Bronx, NY 10458",
            "phone": "(718) 220-0347",
            "rating": 4.4,
            "price_range": "$$",
            "latitude": 40.8589,
            "longitude": -73.8904,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        },
        {
            "id": uuid.uuid4(),
            "name": "Taverna Kyclades",
            "cuisine": "Greek",
            "borough": "Queens",
            "address": "36-01 Ditmars Blvd, Astoria, NY 11105",
            "phone": "(718) 545-8666",
            "rating": 4.5,
            "price_range": "$$",
            "latitude": 40.7647,
            "longitude": -73.9105,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        },
        {
            "id": uuid.uuid4(),
            "name": "Randazzo's Clam Bar",
            "cuisine": "Seafood",
            "borough": "Staten Island",
            "address": "2017 Hylan Blvd, Staten Island, NY 10306",
            "phone": "(718) 667-0339",
            "rating": 4.2,
            "price_range": "$$",
            "latitude": 40.5703,
            "longitude": -74.1176,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
    ]
    
    # Inserir dados
    for restaurant in restaurants_data:
        insert_query = """
        INSERT INTO nyc_restaurants (
            id, name, cuisine, borough, address, phone, rating, 
            price_range, latitude, longitude, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        await connection.execute_async(
            insert_query,
            (
                restaurant["id"], restaurant["name"], restaurant["cuisine"],
                restaurant["borough"], restaurant["address"], restaurant["phone"],
                restaurant["rating"], restaurant["price_range"], restaurant["latitude"],
                restaurant["longitude"], restaurant["created_at"], restaurant["updated_at"]
            )
        )
    
    console.print(f"[green]✅ {len(restaurants_data)} restaurantes inseridos com sucesso![/green]")
    console.print("[bold green]🎉 Migração NYC Restaurants aplicada com sucesso![/bold green]")

async def downgrade():
    """Reverte as mudanças desta migração."""
    console.print(f"[bold yellow]Revertendo migração: nyc_restaurants_data[/bold yellow]")
    
    # Remover a tabela
    drop_table_query = "DROP TABLE IF EXISTS nyc_restaurants"
    await connection.execute_async(drop_table_query)
    
    console.print("[green]✅ Tabela nyc_restaurants removida com sucesso![/green]")
    console.print("[bold green]🎉 Migração NYC Restaurants revertida com sucesso![/bold green]")
