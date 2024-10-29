import pandas as pd
from redis.commands.search.reducers import tolist
from sqlalchemy import create_engine, text, column

import cred_pg as c

engine = create_engine(
    f'postgresql+psycopg://{c.pg_userid}:{c.pg_password}@{c.pg_host}/{c.pg_db}',
    connect_args={
        'options': '-c search_path=${user},ugeobln,ugm,uinsta,umisc,umobility,usozmed,public',
        'keepalives_idle': 120
    },
    pool_size=1,
    max_overflow=0,
    execution_options={'isolation_level': 'AUTOCOMMIT'}
)


def emit_start():
    return """
        @startuml
        ' hide the spot
        hide circle

        ' avoid problems with angled crows feet
        skinparam linetype ortho
    """


def emit_end():
    return """
        @enduml
    """


def build_entity_entry(table_name, df_table, df_keys):
    entity_string = f"\n\tentity \"{table_name}\" as {table_name}"
    entity_string += " {"

    for index, row in df_table.iterrows():
        if index != 0:
            entity_string += ("\n\t\t--")

        name = row['column_name']

        entity_string += (
            f"\n\t\t{name}: "
            f"{"nullable" if row['is_nullable'] == "YES" else "non-nullable"}: "
            f"{row['data_type']} "
        )

        for index, row in df_keys.iterrows():
            if row['column_name'] == name:
                if row['contype'] == 'f':
                    entity_string += "FK "

                if row['contype'] == 'p':
                    entity_string += "PK "

    entity_string += "\n\t}"

    return entity_string


def er_diagram(schema):
    result_string = f"{emit_start()}\n"

    with engine.connect() as con:
        sql = f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema}' ORDER BY tablename"
        df = pd.read_sql_query(text(sql), con)

        relation_list = []

        for table in df.tablename.values:
            sql_table = f"SELECT * FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}'"
            df_table = pd.read_sql_query(sql_table, con)

            sql_keys = f"""
                SELECT constraint_name, table_name, column_name, contype
                FROM information_schema.key_column_usage
                INNER JOIN pg_constraint ON information_schema.key_column_usage.constraint_name = conname
                WHERE (table_name = '{table}')
                    AND (contype = 'p'
                    OR contype = 'f')
                """
            df_keys = pd.read_sql_query(sql_keys, con)

            sql_key_relations = f"""
                SELECT con.conname AS constraint_name,
                    conrel.relname AS table_name,
                    att2.attname AS column_name,
                    confrel.relname AS referenced_table,
                    att.attname AS referenced_column
                FROM pg_constraint con
                    INNER JOIN pg_class conrel ON con.conrelid = conrel.oid
                    INNER JOIN pg_namespace nsp ON conrel.relnamespace = nsp.oid
                    INNER JOIN pg_attribute att2 ON att2.attnum = ANY (con.conkey) AND att2.attrelid = conrel.oid
                    INNER JOIN pg_class confrel ON con.confrelid = confrel.oid
                    INNER JOIN pg_attribute att ON att.attnum = ANY (con.confkey) AND att.attrelid = confrel.oid
                WHERE
                    con.contype = 'f'
                    AND nsp.nspname = '{schema}'
                    AND conrel.relname = '{table}';
                """
            df_key_relations = pd.read_sql_query(sql_key_relations, con)

            for index, row in df_key_relations.iterrows():
                relation_list.append(f"\n\t{row['table_name']}||--o{{{row['referenced_table']}")

            result_string += build_entity_entry(table, df_table, df_keys)

        for item in relation_list:
            result_string += f"\n\t{item}"

        result_string += emit_end()
        return result_string


print(er_diagram("umobility"))
