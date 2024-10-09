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
            entity_string += ("\t\t\n--")

        name = row['column_name']

        entity_string += (
            f"\n\t\t{name}: "
            f"{"nullable" if row['is_nullable'] == "YES" else "non-nullable"}: "
            f"{row['data_type']} "
        )

        for index, row in df_keys.iterrows():
            if row['column_name'] == name:
                if row['contype'] == 'f':
                    entity_string += "FOREIGN KEY"

                if row['contype'] == 'p':
                    entity_string += "PRIMARY KEY"

        # row_keys = df_keys.loc[df_keys['column_name'] == name]['contype']
        # if not row_keys.empty:
        #    print(row_keys)
        #    if row_keys == 'f':
        #       entity_string += "FOREIGN KEY"

        # if row_keys.get('contype') == 'p':
        #    entity_string += "PRIMARY KEY"

    entity_string += "\n\t}"
    return entity_string


def er_diagram(schema):
    result_string = f"{emit_start()}\n"

    with engine.connect() as con:
        sql = f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema}' ORDER BY tablename"
        df = pd.read_sql_query(text(sql), con)
        # print(df)

        for table in df.tablename.values:
            sql_table = f"SELECT * FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}'"
            sql_keys = f"""
            SELECT constraint_name, table_name, column_name, contype
FROM information_schema.key_column_usage
         INNER JOIN pg_constraint ON information_schema.key_column_usage.constraint_name = conname
WHERE (table_name = '{table}')
  AND (contype = 'p'
    OR contype = 'f')
"""
            # AND NOT column_name = 'ulid';
            df_table = pd.read_sql_query(sql_table, con)
            df_keys = pd.read_sql_query(sql_keys, con)
            result_string += build_entity_entry(table, df_table, df_keys)

        result_string += emit_end()
        return result_string


print(er_diagram("umobility"))
