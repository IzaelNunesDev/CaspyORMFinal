# caspyorm/connection.py

import asyncio
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from typing import List, Optional, Dict, Any
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConnectionManager:
    """Gerencia a conexão com o cluster Cassandra."""
    
    def __init__(self):
        self.cluster: Optional[Cluster] = None
        self.session = None
        self.async_session = None  # Sessão para operações assíncronas
        self.keyspace: Optional[str] = None
        self._is_connected = False
        self._is_async_connected = False  # Flag para conexão assíncrona
    
    def connect(
        self, 
        contact_points: List[str] = ['127.0.0.1'],
        port: int = 9042,
        keyspace: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs: Any
    ) -> None:
        """
        Conecta ao cluster Cassandra (síncrono).
        
        Args:
            contact_points: Lista de endereços dos nós do cluster
            port: Porta do Cassandra (padrão: 9042)
            keyspace: Keyspace para usar
            username: Usuário para autenticação (opcional)
            password: Senha para autenticação (opcional)
        """
        try:
            # Configurar autenticação se fornecida
            auth_provider = None
            if username and password:
                auth_provider = PlainTextAuthProvider(username=username, password=password)
            
            # Criar cluster
            self.cluster = Cluster(
                contact_points=contact_points,
                port=port,
                auth_provider=auth_provider,
                **kwargs
            )
            
            # Conectar e obter sessão
            self.session = self.cluster.connect()
            self._is_connected = True
    
            # Usar keyspace se especificado
            if keyspace:
                self.use_keyspace(keyspace)
            
            logger.info(f"Conectado ao Cassandra (SÍNCRONO) em {contact_points}:{port}")
            
        except Exception as e:
            logger.error(f"Erro ao conectar ao Cassandra: {e}")
            raise

    async def connect_async(
        self, 
        contact_points: List[str] = ['127.0.0.1'],
        port: int = 9042,
        keyspace: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs: Any
    ) -> None:
        """
        Conecta ao cluster Cassandra (assíncrono).
        """
        try:
            # Configurar autenticação se fornecida
            auth_provider = None
            if username and password:
                auth_provider = PlainTextAuthProvider(username=username, password=password)
            
            # Criar cluster
            self.cluster = Cluster(
                contact_points=contact_points,
                port=port,
                auth_provider=auth_provider,
                **kwargs
            )
            
            # Conectar e obter sessão
            self.async_session = await self.cluster.connect_async()
            self._is_async_connected = True
    
            # Usar keyspace se especificado
            if keyspace:
                await self.use_keyspace_async(keyspace)
            
            logger.info(f"Conectado ao Cassandra (ASSÍNCRONO) em {contact_points}:{port}")
            
        except Exception as e:
            logger.error(f"Erro ao conectar ao Cassandra (async): {e}")
            raise
    
    def use_keyspace(self, keyspace: str) -> None:
        """Define o keyspace ativo (síncrono)."""
        if not self.session:
            raise RuntimeError("Não há conexão ativa com o Cassandra")
        
        try:
            # Criar keyspace se não existir
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)
            
            # Usar o keyspace
            self.session.set_keyspace(keyspace)
            self.keyspace = keyspace
            
            logger.info(f"Usando keyspace (SÍNCRONO): {keyspace}")
            
        except Exception as e:
            logger.error(f"Erro ao usar keyspace {keyspace}: {e}")
            raise

    async def use_keyspace_async(self, keyspace: str) -> None:
        """Define o keyspace ativo (assíncrono)."""
        if not self.async_session:
            raise RuntimeError("Não há conexão assíncrona ativa com o Cassandra")
        
        try:
            # Criar keyspace se não existir
            await self.execute_async(f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)
            
            # Usar o keyspace
            self.async_session.set_keyspace(keyspace)
            self.keyspace = keyspace
            
            logger.info(f"Usando keyspace (ASSÍNCRONO): {keyspace}")
            
        except Exception as e:
            logger.error(f"Erro ao usar keyspace {keyspace} (async): {e}")
            raise


    
    def execute(self, query: str, parameters: Optional[Any] = None):
        """Executa uma query CQL (síncrono)."""
        if not self.session:
            raise RuntimeError("Não há conexão ativa com o Cassandra")
        try:
            if parameters is not None:
                return self.session.execute(query, parameters)
            else:
                return self.session.execute(query)
        except Exception as e:
            logger.error(f"Erro ao executar query: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Parâmetros: {parameters}")
            raise

    async def execute_async(self, query: str, parameters: Optional[Any] = None):
        """Executa uma query CQL (assíncrono)."""
        if not self.async_session:
            raise RuntimeError("Não há conexão assíncrona ativa com o Cassandra")
        try:
            if parameters is not None:
                future = self.async_session.execute_async(query, parameters)
            else:
                future = self.async_session.execute_async(query)
            return await future
        except Exception as e:
            logger.error(f"Erro ao executar query (async): {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Parâmetros: {parameters}")
            raise
    
    def disconnect(self) -> None:
        """Desconecta do cluster Cassandra (síncrono)."""
        if self.session:
            self.session.shutdown()
            self.session = None
        
        if self.cluster:
            self.cluster.shutdown()
            self.cluster = None
        
        self._is_connected = False
        self.keyspace = None
        
        logger.info("Desconectado do Cassandra (SÍNCRONO)")

    async def disconnect_async(self) -> None:
        """Desconecta do cluster Cassandra (assíncrono)."""
        if self.async_session:
            await self.async_session.shutdown_async()
            self.async_session = None
        
        if self.cluster:
            await self.cluster.shutdown_async()
            self.cluster = None
        
        self._is_async_connected = False
        self.keyspace = None
        
        logger.info("Desconectado do Cassandra (ASSÍNCRONO)")
    
    @property
    def is_connected(self) -> bool:
        """Verifica se há uma conexão ativa (síncrona)."""
        return self._is_connected and self.session is not None

    @property
    def is_async_connected(self) -> bool:
        """Verifica se há uma conexão assíncrona ativa."""
        return self._is_async_connected and self.async_session is not None
    
    def get_cluster(self) -> Optional[Cluster]:
        """Retorna a instância do cluster ativo."""
        return self.cluster
    
    def get_session(self):
        """
        Retorna a sessão ativa do Cassandra (síncrona).
        Garante que a conexão foi estabelecida.
        """
        if not self.session or not self._is_connected:
            raise RuntimeError("A conexão com o Cassandra não foi estabelecida. Chame `connection.connect()` primeiro.")
        return self.session

    def get_async_session(self):
        """
        Retorna a sessão assíncrona ativa do Cassandra.
        Garante que a conexão assíncrona foi estabelecida.
        """
        if not self.async_session or not self._is_async_connected:
            raise RuntimeError("A conexão assíncrona com o Cassandra não foi estabelecida. Chame `connection.connect_async()` primeiro.")
        return self.async_session

# Instância global do gerenciador de conexão
connection = ConnectionManager()

# Funções de conveniência (síncronas)
def connect(**kwargs):
    """Conecta ao Cassandra usando a instância global (síncrono)."""
    connection.connect(**kwargs)

def disconnect():
    """Desconecta do Cassandra usando a instância global (síncrono)."""
    connection.disconnect()

def execute(query: str, parameters: Optional[Any] = None):
    """Executa uma query usando a instância global (síncrono)."""
    return connection.execute(query, parameters)

def get_cluster() -> Optional[Cluster]:
    """Retorna a instância do cluster ativo."""
    return connection.get_cluster()

def get_session():
    """
    Retorna a sessão ativa do Cassandra (síncrona).
    Garante que a conexão foi estabelecida.
    """
    return connection.get_session()

# Funções de conveniência (assíncronas)
async def connect_async(**kwargs):
    """Conecta ao Cassandra usando a instância global (assíncrono)."""
    await connection.connect_async(**kwargs)

async def disconnect_async():
    """Desconecta do Cassandra usando a instância global (assíncrono)."""
    await connection.disconnect_async()

async def execute_async(query: str, parameters: Optional[Any] = None):
    """Executa uma query usando a instância global (assíncrono)."""
    return await connection.execute_async(query, parameters)

def get_async_session():
    """
    Retorna a sessão assíncrona ativa do Cassandra.
    Garante que a conexão assíncrona foi estabelecida.
    """
    return connection.get_async_session()