# caspyorm/connection.py

import asyncio
import logging
from typing import Any, Dict, List, Optional, Type

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import BoundStatement, PreparedStatement

from ..utils.exceptions import ConnectionError, QueryError

# Adiciona suporte ao aiocassandra para integração nativa com asyncio
try:
    import aiocassandra

    _AIO_CASSANDRA_AVAILABLE = True

    def _patch_aiocassandra(session):
        aiocassandra.aiosession(session)

except ImportError:
    _AIO_CASSANDRA_AVAILABLE = False

    def _patch_aiocassandra(session):
        pass


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
        self._prepared_statement_cache = {}  # Cache de prepared statements
        self._registered_udts: Dict[str, Type] = {}  # Cache de UDTs registrados

    def register_udt(self, udt_class: Type) -> None:
        """
        Registra um User-Defined Type para sincronização automática.

        Args:
            udt_class: Classe que herda de UserType
        """
        from ..types.usertype import UserType

        if not isinstance(udt_class, type) or not issubclass(udt_class, UserType):
            raise TypeError("UDT deve herdar de UserType")

        type_name = getattr(udt_class, "__type_name__", udt_class.__name__.lower())
        self._registered_udts[type_name] = udt_class
        logger.info(f"UDT registrado: {type_name} -> {udt_class.__name__}")

    def sync_udts(self) -> None:
        """Sincroniza todos os UDTs registrados com o Cassandra."""
        if not self.session or not self.keyspace:
            raise RuntimeError("Deve estar conectado e com keyspace definido")

        for type_name, udt_class in self._registered_udts.items():
            self._create_udt_if_not_exists(type_name, udt_class)

    async def sync_udts_async(self) -> None:
        """Sincroniza todos os UDTs registrados com o Cassandra (assíncrono)."""
        if not self.async_session or not self.keyspace:
            raise RuntimeError("Deve estar conectado e com keyspace definido")

        for type_name, udt_class in self._registered_udts.items():
            await self._create_udt_if_not_exists_async(type_name, udt_class)

    def _create_udt_if_not_exists(self, type_name: str, udt_class: Type) -> None:
        """Cria um UDT no Cassandra se não existir."""
        if not self.session:
            raise RuntimeError("Não há conexão ativa com o Cassandra")

        try:
            # Construir definição dos campos
            field_definitions = []
            for field_name, field_obj in udt_class.model_fields.items():
                field_def = f"{field_name} {field_obj.get_cql_definition()}"
                field_definitions.append(field_def)

            # Query para criar UDT
            fields_str = ", ".join(field_definitions)
            create_udt_query = f"""
                CREATE TYPE IF NOT EXISTS {self.keyspace}.{type_name} (
                    {fields_str}
                )
            """

            self.session.execute(create_udt_query)
            logger.info(f"UDT criado/verificado: {type_name}")

        except Exception as e:
            logger.error(f"Erro ao criar UDT {type_name}: {e}")
            raise

    async def _create_udt_if_not_exists_async(
        self, type_name: str, udt_class: Type
    ) -> None:
        """Cria um UDT no Cassandra se não existir (assíncrono)."""
        try:
            # Construir definição dos campos
            field_definitions = []
            for field_name, field_obj in udt_class.model_fields.items():
                field_def = f"{field_name} {field_obj.get_cql_definition()}"
                field_definitions.append(field_def)

            # Query para criar UDT
            fields_str = ", ".join(field_definitions)
            create_udt_query = f"""
                CREATE TYPE IF NOT EXISTS {self.keyspace}.{type_name} (
                    {fields_str}
                )
            """

            await self.execute_async(create_udt_query)
            logger.info(f"UDT criado/verificado (async): {type_name}")

        except Exception as e:
            logger.error(f"Erro ao criar UDT {type_name} (async): {e}")
            raise

    def connect(
        self,
        contact_points: List[str] = ["cassandra_nyc"],
        port: int = 9042,
        keyspace: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Conecta ao cluster Cassandra (síncrono).
        """
        try:
            auth_provider = None
            if username and password:
                auth_provider = PlainTextAuthProvider(
                    username=username, password=password
                )
            self.cluster = Cluster(
                contact_points=contact_points,
                port=port,
                auth_provider=auth_provider,
                **kwargs,
            )
            self.session = self.cluster.connect()
            self.async_session = self.session  # Compatibilidade
            self._is_connected = True
            self._is_async_connected = True
            if keyspace:
                self.use_keyspace(keyspace)
            logger.info(f"Conectado ao Cassandra (SÍNCRONO) em {contact_points}:{port}")
        except Exception as e:
            logger.error(f"Erro ao conectar ao Cassandra: {e}")
            raise ConnectionError(str(e))

    async def connect_async(
        self,
        contact_points: List[str] = ["cassandra_nyc"],
        port: int = 9042,
        keyspace: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        [DESABILITADO] O suporte assíncrono está desativado devido à incompatibilidade do driver com Cassandra 4.x.
        """
        raise NotImplementedError(
            "Suporte assíncrono desativado devido à incompatibilidade do driver com Cassandra 4.x"
        )

    def use_keyspace(self, keyspace: str) -> None:
        """Define o keyspace ativo (síncrono)."""
        if not self.session:
            raise RuntimeError("Não há conexão ativa com o Cassandra")

        try:
            # Criar keyspace se não existir
            self.session.execute(
                f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """
            )

            # Usar o keyspace
            self.session.set_keyspace(keyspace)
            self.keyspace = keyspace
            # Sincronizar UDTs automaticamente (igual ao async)
            self.sync_udts()
            logger.info(f"Usando keyspace (SÍNCRONO): {keyspace}")

        except Exception as e:
            logger.error(f"Erro ao usar keyspace {keyspace}: {e}")
            raise QueryError(str(e))

    async def use_keyspace_async(self, keyspace: str) -> None:
        """
        [DESABILITADO] O suporte assíncrono está desativado devido à incompatibilidade do driver com Cassandra 4.x.
        """
        raise NotImplementedError(
            "Suporte assíncrono desativado devido à incompatibilidade do driver com Cassandra 4.x"
        )

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
            raise QueryError(str(e))

    async def execute_async(self, query: str, parameters: Optional[Any] = None):
        """
        [DESABILITADO] O suporte assíncrono está desativado devido à incompatibilidade do driver com Cassandra 4.x.
        """
        raise NotImplementedError(
            "Suporte assíncrono desativado devido à incompatibilidade do driver com Cassandra 4.x"
        )

    async def prepare_async(self, cql_query: str) -> PreparedStatement:
        """
        [DESABILITADO] O suporte assíncrono está desativado devido à incompatibilidade do driver com Cassandra 4.x.
        """
        raise NotImplementedError(
            "Suporte assíncrono desativado devido à incompatibilidade do driver com Cassandra 4.x"
        )

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
        """
        [DESABILITADO] O suporte assíncrono está desativado devido à incompatibilidade do driver com Cassandra 4.x.
        """
        raise NotImplementedError(
            "Suporte assíncrono desativado devido à incompatibilidade do driver com Cassandra 4.x"
        )

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
            raise RuntimeError(
                "A conexão com o Cassandra não foi estabelecida. Chame `connection.connect()` primeiro."
            )
        return self.session

    def get_async_session(self):
        """
        Retorna a sessão assíncrona ativa do Cassandra.
        Garante que a conexão assíncrona foi estabelecida.
        """
        if not self.async_session or not self._is_async_connected:
            raise RuntimeError(
                "A conexão assíncrona com o Cassandra não foi estabelecida. Chame `connection.connect_async()` primeiro."
            )
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


async def prepare_async(cql_query: str) -> PreparedStatement:
    """Prepara uma query usando a instância global (assíncrono)."""
    return await connection.prepare_async(cql_query)


def get_async_session():
    """
    Retorna a sessão assíncrona ativa do Cassandra.
    Garante que a conexão assíncrona foi estabelecida.
    """
    return connection.get_async_session()


async def execute_cql_async(query, parameters: Optional[Any] = None):
    """Helper para executar queries CQL de forma assíncrona usando asyncio.to_thread.
    Aceita str, PreparedStatement ou BoundStatement.
    """
    session = get_async_session()
    if isinstance(query, (PreparedStatement, BoundStatement)):
        if parameters is not None:
            future = session.execute_async(query, parameters)
        else:
            future = session.execute_async(query)
    else:
        # query é str
        if parameters is not None:
            future = session.execute_async(query, parameters)
        else:
            future = session.execute_async(query)
    return await asyncio.to_thread(future.result)
