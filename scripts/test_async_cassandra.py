import asyncio
from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection
from aiocassandra import aiosession

async def main():
    cluster = Cluster(contact_points=["172.18.0.2"], port=9042, connection_class=AsyncioConnection)
    try:
        session = cluster.connect()
        aiosession(session)
        print("Conexão assíncrona estabelecida com sucesso!")
        # Testa uma query simples
        rows = await session.execute_async("SELECT release_version FROM system.local")
        for row in rows.current_rows:
            print("Versão do Cassandra:", row.release_version)
    except Exception as e:
        print("Erro ao conectar de forma assíncrona:", e)
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    asyncio.run(main()) 