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


def emit_entity(entity):
    return f'''
        entity "{entity}" as {entity} {{
          id 
          --
          attributes
        }}
    '''


def emit_entities(entities):
    return "".join([emit_entity(entity) for entity in entities])


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


def build_entity_relation(df_keys):
    relation_string = "\n"

    df_primary = df_keys.loc[df_keys['contype'] == 'p']
    df_foreign = df_keys.loc[df_keys['contype'] == 'f']

    for index, row in df_foreign.iterrows():
        for index2, row2 in df_primary.iterrows():
            if row['column_name'] == row2['column_name']:
                relation_string += f"\n\t\t{row2['tablename']}||--|{{{row['tablename']}"

    return relation_string


def er_diagram(schema):
    result_string = f"{emit_start()}\n"

    with engine.connect() as con:
        sql = f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema}' ORDER BY tablename"
        df = pd.read_sql_query(text(sql), con)

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
            # AND NOT column_name = 'ulid';
            df_keys = pd.read_sql_query(sql_keys, con)

            result_string += build_entity_entry(table, df_table, df_keys)

        sql_keys = f"""
            SELECT pgt.tablename, iskcu.constraint_name, iskcu.column_name, pgc.contype
            FROM pg_tables pgt
                INNER JOIN information_schema.key_column_usage iskcu ON pgt.tablename=iskcu.table_name
                INNER JOIN pg_constraint pgc ON iskcu.constraint_name = conname
            WHERE schemaname = '{schema}'
            """
        # AND NOT column_name = 'ulid';
        df_keys2 = pd.read_sql_query(sql_keys, con)

        result_string += build_entity_relation(df_keys2)
        result_string += emit_end()
        return result_string


print(er_diagram("umobility"))
