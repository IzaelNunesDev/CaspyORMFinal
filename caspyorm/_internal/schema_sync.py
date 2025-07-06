# caspyorm/_internal/schema_sync.py
import logging
from typing import Any, Dict, List, Optional, Type, TYPE_CHECKING

from ..connection import get_session

if TYPE_CHECKING:
    from cassandra.cluster import Session
    from ..model import Model
else:
    Session = Any
    Model = Any

logger = logging.getLogger(__name__)

def _get_cql_type(field_type: str) -> str:
    """Mapeia tipos Python para tipos CQL."""
    type_mapping = {
        'text': 'text',
        'varchar': 'text',
        'int': 'int',
        'bigint': 'bigint',
        'float': 'float',
        'double': 'double',
        'boolean': 'boolean',
        'uuid': 'uuid',
        'timestamp': 'timestamp',
        'date': 'date',
        'time': 'time',
        'blob': 'blob',
        'decimal': 'decimal',
        'varint': 'int',
        'inet': 'inet',
        'list': 'list<text>',
        'set': 'set<text>',
        'map': 'map<text, text>',
        'tuple': 'tuple<text>',
        'frozen': 'frozen<text>',
        'counter': 'counter',
        'duration': 'duration',
        'smallint': 'int',
        'tinyint': 'int',
        'timeuuid': 'uuid',
        'ascii': 'text',
        'json': 'text'
    }
    base_type = field_type.split('<')[0].split('(')[0].lower()
    return type_mapping.get(base_type, 'text')

def get_cassandra_table_schema(session: Session, keyspace: str, table_name: str) -> Optional[Dict[str, Any]]:
    """
    Obtém o schema atual de uma tabela no Cassandra.
    Retorna None se a tabela não existir.
    """
    try:
        # Consulta sem ORDER BY, pois Cassandra não permite ORDER BY em system_schema.columns
        query = f"""
            SELECT column_name, kind, type
            FROM system_schema.columns
            WHERE keyspace_name = '{keyspace}'
            AND table_name = '{table_name}'
        """
        rows = session.execute(query)
        if not rows:
            return None
        
        # Estrutura para armazenar o schema
        schema = {
            'fields': {},
            'primary_keys': [],
            'partition_keys': [],
            'clustering_keys': []
        }
        
        for row in rows:
            column_name = row.column_name
            column_type = row.type
            column_kind = row.kind
            
            # Mapear tipos CQL para tipos Python
            type_mapping = {
                'text': 'text',
                'varchar': 'text',
                'int': 'int',
                'bigint': 'int',
                'float': 'float',
                'double': 'float',
                'boolean': 'boolean',
                'uuid': 'uuid',
                'timestamp': 'timestamp',
                'date': 'date',
                'time': 'time',
                'blob': 'blob',
                'decimal': 'decimal',
                'varint': 'int',
                'inet': 'inet',
                'list': 'list',
                'set': 'set',
                'map': 'map',
                'tuple': 'tuple',
                'frozen': 'frozen',
                'counter': 'counter',
                'duration': 'duration',
                'smallint': 'int',
                'tinyint': 'int',
                'timeuuid': 'uuid',
                'ascii': 'text',
                'json': 'text'
            }
            
            # Simplificar tipos complexos para comparação
            base_type = column_type.split('<')[0].split('(')[0].lower()
            mapped_type = type_mapping.get(base_type, base_type)
            
            schema['fields'][column_name] = {
                'type': mapped_type,
                'cql_type': column_type,
                'kind': column_kind
            }
            
            # Classificar chaves
            if column_kind == 'partition_key':
                schema['partition_keys'].append(column_name)
                schema['primary_keys'].append(column_name)
            elif column_kind == 'clustering':
                schema['clustering_keys'].append(column_name)
                schema['primary_keys'].append(column_name)
        
        return schema
        
    except Exception as e:
        logger.error(f"Erro ao obter schema da tabela {table_name}: {e}")
        return None

def apply_schema_changes(session: Session, table_name: str, model_schema: Dict[str, Any], db_schema: Dict[str, Any]) -> None:
    """
    Aplica as mudanças necessárias no schema da tabela.
    """
    logger.info("\n🚀 Aplicando alterações no schema...")
    
    # Adicionar novas colunas
    for field_name, field_details in model_schema['fields'].items():
        if field_name not in db_schema['fields']:
            cql_type = _get_cql_type(field_details['type'])
            cql = f"ALTER TABLE {table_name} ADD {field_name} {cql_type}"
            try:
                session.execute(cql)
                logger.info(f"  [+] Executando: {cql}")
            except Exception as e:
                logger.error(f"  [!] ERRO ao adicionar coluna '{field_name}': {e}")
    
    # Remover colunas (não suportado automaticamente por segurança)
    for field_name in db_schema['fields']:
        if field_name not in model_schema['fields']:
            logger.warning("\n  [!] AVISO: A remoção automática de colunas não é suportada por segurança.")
            logger.warning(f"      - Operação manual necessária: ALTER TABLE {table_name} DROP {field_name};")
    
    # Verificar mudanças de tipo (não suportado automaticamente)
    for field_name in model_schema['fields']:
        if field_name in db_schema['fields']:
            model_type = model_schema['fields'][field_name]['type']
            db_type = db_schema['fields'][field_name]['type']
            if model_type != db_type:
                mismatch = f"{field_name}: {db_type} -> {model_type}"
                logger.warning("\n  [!] AVISO: A alteração automática de tipo de coluna não é suportada.")
                logger.warning(f"      - Operação manual necessária para: {mismatch}")
    
    # Verificar mudanças na chave primária (não suportado)
    if model_schema['primary_keys'] != db_schema['primary_keys']:
        mismatch = f"{db_schema['primary_keys']} -> {model_schema['primary_keys']}"
        logger.error("\n  [!] ERRO CRÍTICO: A alteração de chave primária não é possível no Cassandra.")
        logger.error("      - A tabela deve ser recriada para aplicar esta mudança.")
    
    logger.info("\n✅ Aplicação de schema concluída.")

def build_create_table_cql(table_name: str, schema: Dict[str, Any]) -> str:
    """
    Constrói a query CQL para criar uma tabela.
    """
    fields = []
    for field_name, field_details in schema['fields'].items():
        field_def = f"{field_name} {field_details['type']}"
        fields.append(field_def)
    
    # Construir chave primária
    if schema['partition_keys'] and schema['clustering_keys']:
        # Chave composta: partition + clustering
        pk_def = f"PRIMARY KEY (({', '.join(schema['partition_keys'])})"
        if schema['clustering_keys']:
            pk_def += f", {', '.join(schema['clustering_keys'])})"
        else:
            pk_def += ")"
    elif schema['partition_keys']:
        # Chave simples
        pk_def = f"PRIMARY KEY ({', '.join(schema['partition_keys'])})"
    else:
        raise RuntimeError("Tabela deve ter pelo menos uma chave primária")
    
    fields.append(pk_def)
    
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(fields)}
    )
    """

def build_create_index_cql(table_name: str, field_name: str) -> str:
    """Constrói a query CREATE INDEX para um campo."""
    index_name = f"{table_name}_{field_name}_idx"
    return f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({field_name});"

def get_existing_indexes(session: Session, keyspace: str, table_name: str) -> set:
    """Obtém os índices existentes para uma tabela."""
    try:
        query = f"""
            SELECT index_name FROM system_schema.indexes
            WHERE keyspace_name = '{keyspace}'
            AND table_name = '{table_name}'
        """
        result = session.execute(query)
        return {row.index_name for row in result}
    except Exception as e:
        logger.warning(f"Erro ao obter índices existentes: {e}")
        return set()

def create_indexes_for_table(session: Session, table_name: str, model_schema: Dict[str, Any], verbose: bool = True) -> None:
    """Cria os índices necessários para uma tabela."""
    if not model_schema.get('indexes'):
        return
    
    keyspace = session.keyspace
    if not keyspace:
        logger.error("Keyspace não está definido na sessão")
        return
    existing_indexes = get_existing_indexes(session, keyspace, table_name)
    
    logger.info(f"Criando índices para a tabela '{table_name}'...")
    
    for field_name in model_schema['indexes']:
        index_name = f"{table_name}_{field_name}_idx"
        
        if index_name in existing_indexes:
            if verbose:
                logger.info(f"  [✓] Índice '{index_name}' já existe")
            continue
        
        create_index_query = build_create_index_cql(table_name, field_name)
        try:
            if verbose:
                logger.info(f"  [+] Executando: {create_index_query}")
            session.execute(create_index_query)
            logger.info(f"  [✓] Índice '{index_name}' criado com sucesso")
        except Exception as e:
            logger.error(f"  [!] ERRO ao criar índice '{index_name}': {e}")
            # Não falhar completamente se um índice falhar
            continue
    
    logger.info("Criação de índices concluída.")

def sync_table(model_cls: Type["Model"], auto_apply: bool = False, verbose: bool = True) -> None:
    """
    Sincroniza o schema do modelo com a tabela no Cassandra.
    
    Args:
        model_cls: Classe do modelo a ser sincronizada
        auto_apply: Se True, aplica as mudanças automaticamente
        verbose: Se True, exibe informações detalhadas
    """
    session = get_session()
    if not session:
        raise RuntimeError("Não há conexão ativa com o Cassandra")
    
    # Obter informações do modelo
    table_name = model_cls.__table_name__
    model_schema = model_cls.__caspy_schema__
    
    # Obter schema atual da tabela
    keyspace = session.keyspace
    if not keyspace:
        raise RuntimeError("Keyspace não está definido na sessão")
    db_schema = get_cassandra_table_schema(session, keyspace, table_name)
    
    if db_schema is None:
        # Tabela não existe, criar
        logger.info(f"Tabela '{table_name}' não encontrada. Criando...")
        create_table_query = build_create_table_cql(table_name, model_schema)
        
        if verbose:
            logger.info(f"Executando CQL para criar tabela:\n{create_table_query}")
        
        try:
            session.execute(create_table_query)
            logger.info("Tabela criada com sucesso.")
            
            # Criar índices após criar a tabela
            create_indexes_for_table(session, table_name, model_schema, verbose)
            
        except Exception as e:
            logger.error(f"Erro ao criar tabela: {e}")
            raise
        return
    
    # Comparar schemas
    model_fields = set(model_schema['fields'].keys())
    db_fields = set(db_schema['fields'].keys())
    
    fields_to_add = model_fields - db_fields
    fields_to_remove = db_fields - model_fields
    fields_to_check = model_fields & db_fields
    
    # Verificar tipos diferentes
    type_mismatches = []
    for field in fields_to_check:
        model_type = model_schema['fields'][field]['type']
        db_type = db_schema['fields'][field]['type']
        if model_type != db_type:
            type_mismatches.append(f"{field}: {db_type} -> {model_type}")
    
    # Verificar chave primária
    pk_mismatch = None
    if model_schema['primary_keys'] != db_schema['primary_keys']:
        pk_mismatch = f"{db_schema['primary_keys']} -> {model_schema['primary_keys']}"
    
    # Verificar se há diferenças
    has_changes = (fields_to_add or fields_to_remove or type_mismatches or pk_mismatch)
    
    if not has_changes:
        logger.info(f"✅ Schema da tabela '{table_name}' está sincronizado.")
        return
    
    # Há diferenças
    logger.warning(f"⚠️  Schema da tabela '{table_name}' está dessincronizado!")
    
    if verbose:
        if fields_to_add:
            logger.info("\n  [+] Campos a serem ADICIONADOS na tabela:")
            for field in fields_to_add:
                logger.info(f"      - {field} (tipo: {model_schema['fields'][field]['type']})")
        
        if fields_to_remove:
            logger.info("\n  [-] Campos a serem REMOVIDOS da tabela:")
            for field in fields_to_remove:
                logger.info(f"      - {field} (tipo: {db_schema['fields'][field]['type']})")
        
        if type_mismatches:
            logger.info("\n  [~] Campos com TIPOS DIFERENTES:")
            for mismatch in type_mismatches:
                logger.info(f"      - {mismatch}")
        
        if pk_mismatch:
            logger.error("\n  [!] Chave primária diferente:")
            logger.error(f"      - {pk_mismatch}")
    
    # Aplicar mudanças se solicitado
    if auto_apply:
        apply_schema_changes(session, table_name, model_schema, db_schema)
        # Criar índices após aplicar mudanças
        create_indexes_for_table(session, table_name, model_schema, verbose)
    else:
        logger.info("\nExecute sync_table(auto_apply=True) para aplicar as mudanças automaticamente.")