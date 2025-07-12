"""
Utilitários para conversão de tipos Python para tipos CQL.
"""


def get_cql_type(field_type: str) -> str:
    """
    Converte o tipo do campo para CQL.

    Args:
        field_type: Tipo do campo (ex: 'text', 'int', 'list<text>')

    Returns:
        Tipo CQL correspondente

    Examples:
        >>> get_cql_type('text')
        'text'
        >>> get_cql_type('list<text>')
        'list<text>'
        >>> get_cql_type('varchar')
        'text'
    """
    # Se o tipo já contém '<', significa que é um tipo composto (list<text>, etc.)
    # e já está no formato correto
    if "<" in field_type:
        return field_type

    type_mapping = {
        "text": "text",
        "varchar": "text",
        "int": "int",
        "bigint": "bigint",
        "float": "float",
        "double": "double",
        "boolean": "boolean",
        "uuid": "uuid",
        "timestamp": "timestamp",
        "date": "date",
        "time": "time",
        "blob": "blob",
        "decimal": "decimal",
        "varint": "varint",
        "inet": "inet",
        "list": "list<text>",
        "set": "set<text>",
        "map": "map<text, text>",
        "tuple": "tuple<text>",
        "frozen": "frozen<text>",
        "counter": "counter",
        "duration": "duration",
        "smallint": "int",
        "tinyint": "int",
        "timeuuid": "uuid",
        "ascii": "text",
        "json": "text",
    }

    base_type = field_type.split("<")[0].split("(")[0].lower()
    return type_mapping.get(base_type, "text")


def get_python_type_mapping() -> dict:
    """
    Retorna o mapeamento de tipos CQL para tipos Python.

    Returns:
        Dicionário com mapeamento de tipos CQL para tipos Python
    """
    return {
        "text": "text",
        "varchar": "text",
        "int": "int",
        "bigint": "int",
        "float": "float",
        "double": "float",
        "boolean": "boolean",
        "uuid": "uuid",
        "timestamp": "timestamp",
        "date": "date",
        "time": "time",
        "blob": "blob",
        "decimal": "decimal",
        "varint": "int",
        "inet": "inet",
        "list": "list",
        "set": "set",
        "map": "map",
        "tuple": "tuple",
        "frozen": "frozen",
        "counter": "counter",
        "duration": "duration",
        "smallint": "int",
        "tinyint": "int",
        "timeuuid": "uuid",
        "ascii": "text",
        "json": "text",
    }


# Alias para compatibilidade
_get_cql_type = get_cql_type
