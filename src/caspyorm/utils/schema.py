from caspyorm._internal.query_builder import build_create_table_cql
from caspyorm.types.usertype import UserType


def create_udt(session, udt_class, keyspace):
    """
    Cria o UDT no Cassandra a partir de uma classe UserType da CaspyORM.
    """
    if not issubclass(udt_class, UserType):
        raise TypeError("A classe deve herdar de UserType da CaspyORM.")
    type_name = getattr(udt_class, "__type_name__", udt_class.__name__.lower())
    fields = []
    for field_name, field_obj in udt_class.model_fields.items():
        cql_type = field_obj.get_cql_definition()
        fields.append(f"{field_name} {cql_type}")
    fields_cql = ", ".join(fields)
    cql = f"CREATE TYPE IF NOT EXISTS {keyspace}.{type_name} ({fields_cql});"
    session.execute(cql)


def create_table(session, model_class):
    """
    Cria a tabela no Cassandra a partir de um modelo CaspyORM.
    """
    cql = build_create_table_cql(model_class.__caspy_schema__)
    session.execute(cql)
