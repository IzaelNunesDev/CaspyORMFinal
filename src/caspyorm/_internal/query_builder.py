# caspyorm/_internal/query_builder.py

import logging
from typing import Any, Dict, List, Optional, Tuple

from .cql_types import get_cql_type

logger = logging.getLogger(__name__)


def build_insert_cql(schema: Dict[str, Any], ttl: Optional[int] = None) -> str:
    """Constrói uma query INSERT com suporte a TTL."""
    table_name = schema["table_name"]
    field_names = list(schema["fields"].keys())

    columns = ", ".join(field_names)
    placeholders = ", ".join(["?"] * len(field_names))
    ttl_clause = f"USING TTL {ttl}" if ttl is not None else ""
    if ttl_clause:
        return (
            f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) {ttl_clause}"
        )
    else:
        return f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"


def build_select_cql(
    schema: Dict[str, Any],
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    ordering: Optional[List[str]] = None,
    allow_filtering: bool = False,
) -> Tuple[str, List[Any]]:
    """Constrói uma query SELECT ... WHERE ... ORDER BY ... LIMIT com suporte a operadores."""
    table_name = schema["table_name"]

    # Seleciona colunas específicas ou '*'
    select_clause = ", ".join(columns) if columns else "*"
    cql = f"SELECT {select_clause} FROM {table_name}"
    params: List[Any] = []

    if filters:
        # Mapeamento de nossos operadores para operadores CQL
        operator_map = {
            "exact": "=",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "in": "IN",
        }

        where_clauses = []
        for key, value in filters.items():
            # Separa o nome do campo e o operador
            parts = key.split("__")
            field_name = parts[0]
            op = parts[1] if len(parts) > 1 else "exact"

            if op not in operator_map:
                raise ValueError(
                    f"Operador de filtro não suportado: '{op}'. Operadores válidos: {list(operator_map.keys())}"
                )

            cql_operator = operator_map[op]

            # O operador IN espera uma tupla de placeholders
            if cql_operator == "IN":
                if not isinstance(value, (list, tuple, set)):
                    raise TypeError(
                        f"O valor para o filtro '__in' deve ser uma lista, tupla ou set, recebido: {type(value)}"
                    )
                placeholders = ", ".join(["?"] * len(value))
                where_clauses.append(f"{field_name} IN ({placeholders})")
                params.extend(value)
            else:
                where_clauses.append(f"{field_name} {cql_operator} ?")
                params.append(value)

        cql += " WHERE " + " AND ".join(where_clauses)

    # --- LÓGICA DE ORDENAÇÃO ---
    if ordering:
        order_clauses = []
        for field in ordering:
            direction = "DESC" if field.startswith("-") else "ASC"
            field_name = field.lstrip("-")

            # Verificar se o campo é uma chave de clusterização (se disponível no schema)
            clustering_keys = schema.get("clustering_keys", [])
            if clustering_keys and field_name not in clustering_keys:
                logger.warning(
                    f"AVISO: Ordenando por '{field_name}', que não é uma chave de clusterização. A query pode falhar se não for permitida."
                )

            order_clauses.append(f"{field_name} {direction}")

        cql += " ORDER BY " + ", ".join(order_clauses)
    # ---------------------------

    if limit:
        cql += " LIMIT ?"
        params.append(limit)

    if allow_filtering:
        cql += " ALLOW FILTERING"

    return cql, params


def build_create_table_cql(schema: Dict[str, Any]) -> str:
    """Constrói a query CREATE TABLE IF NOT EXISTS."""
    table_name = schema["table_name"]
    fields = schema["fields"]
    primary_keys = schema["primary_keys"]

    # Construir definições de colunas (sem PRIMARY KEY aqui)
    column_definitions = []
    for field_name, field_info in fields.items():
        cql_type = get_cql_type(field_info["type"])
        column_def = f"{field_name} {cql_type}"
        column_definitions.append(column_def)

    # Construir a cláusula PRIMARY KEY
    if len(primary_keys) == 1:
        # Chave simples
        pk_clause = f"PRIMARY KEY ({primary_keys[0]})"
    else:
        # Chave composta: primeiro campo é partition key, resto são clustering keys
        partition_key = primary_keys[0]
        clustering_keys = primary_keys[1:] if len(primary_keys) > 1 else []

        if clustering_keys:
            pk_clause = f"PRIMARY KEY ({partition_key}, {', '.join(clustering_keys)})"
        else:
            pk_clause = f"PRIMARY KEY ({partition_key})"

    # Construir a query completa
    columns_str = ", ".join(column_definitions)
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str}, {pk_clause})"

    return query


def build_add_column_cql(table_name: str, column_name: str, column_type: str) -> str:
    """Constrói uma query ALTER TABLE para adicionar uma nova coluna."""
    cql_type = get_cql_type(column_type)
    return f"ALTER TABLE {table_name} ADD {column_name} {cql_type};"


def build_drop_column_cql(table_name: str, column_name: str) -> str:
    """Constrói uma query ALTER TABLE para remover uma coluna."""
    return f"ALTER TABLE {table_name} DROP {column_name};"


def build_count_cql(
    schema: Dict[str, Any], filters: Optional[Dict[str, Any]] = None
) -> Tuple[str, List[Any]]:
    """Constrói uma query SELECT COUNT(*) ... WHERE."""
    table_name = schema["table_name"]

    cql = f"SELECT COUNT(*) FROM {table_name}"
    params: List[Any] = []

    if filters:
        # Mapeamento de nossos operadores para operadores CQL
        operator_map = {
            "exact": "=",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "in": "IN",
        }

        where_clauses = []
        for key, value in filters.items():
            # Separa o nome do campo e o operador
            parts = key.split("__")
            field_name = parts[0]
            op = parts[1] if len(parts) > 1 else "exact"

            if op not in operator_map:
                raise ValueError(
                    f"Operador de filtro não suportado: '{op}'. Operadores válidos: {list(operator_map.keys())}"
                )

            cql_operator = operator_map[op]

            # O operador IN espera uma tupla de placeholders
            if cql_operator == "IN":
                if not isinstance(value, (list, tuple, set)):
                    raise TypeError(
                        f"O valor para o filtro '__in' deve ser uma lista, tupla ou set, recebido: {type(value)}"
                    )
                placeholders = ", ".join(["?"] * len(value))
                where_clauses.append(f"{field_name} IN ({placeholders})")
                params.extend(value)
            else:
                where_clauses.append(f"{field_name} {cql_operator} ?")
                params.append(value)

        cql += " WHERE " + " AND ".join(where_clauses)
        cql += " ALLOW FILTERING"  # Necessário para filtros em campos não-PK

    logger.debug(f"Query COUNT gerada: {cql} com parâmetros: {params}")

    return cql, params


def build_delete_cql(
    schema: Dict[str, Any], filters: Dict[str, Any]
) -> Tuple[str, List[Any]]:
    """Constrói uma query DELETE ... WHERE."""
    table_name = schema["table_name"]
    if not filters:
        raise ValueError(
            "A deleção em massa sem um filtro 'WHERE' não é permitida por segurança."
        )
    # Validação crucial: A deleção no Cassandra DEVE especificar a chave de partição completa.
    partition_keys = set(schema.get("partition_keys", []))
    filter_keys = set(filters.keys())
    if not partition_keys.issubset(filter_keys):
        raise ValueError(
            f"Para deletar, você deve especificar todos os campos da chave de partição. "
            f"Chaves de partição: {list(partition_keys)}. Filtros fornecidos: {list(filter_keys)}"
        )
    where_clauses = " AND ".join([f"{key} = ?" for key in filters.keys()])
    params = list(filters.values())
    cql = f"DELETE FROM {table_name} WHERE {where_clauses}"
    return cql, params


def build_update_cql(
    schema: Dict[str, Any],
    update_data: Dict[str, Any],
    pk_filters: Dict[str, Any],
    ttl: Optional[int] = None,
) -> Tuple[str, List[Any]]:
    """
    Constrói uma query UPDATE ... SET ... WHERE ... [USING TTL].
    """
    table_name = schema["table_name"]

    if not update_data:
        raise ValueError("Nenhum campo fornecido para atualização")

    if not pk_filters:
        raise ValueError("Filtros de chave primária são obrigatórios para UPDATE")

    # Construir cláusula SET
    set_clauses = [f"{field} = ?" for field in update_data.keys()]
    set_clause = ", ".join(set_clauses)

    # Construir cláusula WHERE
    where_clauses = [f"{field} = ?" for field in pk_filters.keys()]
    where_clause = " AND ".join(where_clauses)

    # Construir query completa
    ttl_clause = f"USING TTL {ttl}" if ttl is not None else ""
    if ttl_clause:
        cql = f"UPDATE {table_name} {ttl_clause} SET {set_clause} WHERE {where_clause}"
    else:
        cql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"

    # Parâmetros: primeiro os valores do SET, depois os filtros do WHERE
    params = list(update_data.values()) + list(pk_filters.values())

    logger.debug(f"Query UPDATE gerada: {cql} com parâmetros: {params}")

    return cql, params


def build_collection_update_cql(
    schema: Dict[str, Any],
    field_name: str,
    add: Any,
    remove: Any,
    pk_filters: Dict[str, Any],
) -> Tuple[str, List[Any]]:
    """Constrói uma query UPDATE para adicionar/remover itens de uma coleção."""
    table_name = schema["table_name"]

    if add is None and remove is None:
        raise ValueError("Deve ser fornecido 'add' ou 'remove' para update_collection.")

    set_clauses = []
    params = []

    if add:
        set_clauses.append(f"{field_name} = {field_name} + ?")
        params.append(list(add))  # Cassandra espera uma lista para o operador '+'

    if remove:
        set_clauses.append(f"{field_name} = {field_name} - ?")
        params.append(list(remove))  # E uma lista para o operador '-'

    set_clause = ", ".join(set_clauses)

    where_clauses = [f"{field} = ?" for field in pk_filters.keys()]
    where_clause = " AND ".join(where_clauses)

    params.extend(pk_filters.values())

    cql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"

    logger.debug(f"Query de update de coleção gerada: {cql} com parâmetros: {params}")
    return cql, params
