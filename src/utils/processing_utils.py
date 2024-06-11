from src.utils.connection_utils import get_db_credentials, create_engine_connection
import pandas as pd
from sqlalchemy import text

def process_query_with_engine(engine, query):
    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()
    return result, rows

def query_function(credentials="user",
                   database_name=None, 
                   select_columns=None,
                   id_column=None
                   ):

    columns_str = ", ".join([f'"{column}"' for column in select_columns])
    creds = get_db_credentials(f"{credentials}")
    query = f'SELECT DISTINCT {columns_str} FROM {database_name};'
    engine = create_engine_connection(creds, switch=False)

    result, rows = process_query_with_engine(engine, query)

    dataframe = pd.DataFrame(rows, columns=result.keys())
    
    dataframe.insert(0, f"{id_column}_id", range(1, 1 + len(dataframe)))
    
    return dataframe

def collect_queries(query_input):

    credentials = query_input["credentials"]
    database_name = query_input["database_name"]
    queries = query_input["queries"]
    
    dataframe_dict = {}

    for id_name, query_list in queries.items():
        dataframe = query_function(credentials, database_name, query_list, id_name)
        dataframe_dict[id_name] = dataframe

    return dataframe_dict

def rename_column(processed_dataframe, merge_on):
    processed_dataframe.rename(columns={
            f"{merge_on}_id": f"{merge_on.lower()}_id",
        }, inplace=True)
    return processed_dataframe
    
def merge_dataframe(dataframe_dict, to_merge, merge_on, how="left"):
    processed_df = dataframe_dict[to_merge]
    on = list(dataframe_dict[merge_on].columns[1:])
    suffixes = tuple(f"'', '_{merge_on}'")
    processed_df = processed_df.merge(dataframe_dict[merge_on], on=on, how=how, suffixes=suffixes)

    output_df = rename_column(processed_df, merge_on)
    return output_df

def gather_tables(query_input):
    tables_to_process = []
    for key, _ in query_input["queries"].items():
        tables_to_process.append(key)

    return tables_to_process

def rename_table_columns(dataframe, table):
    for col in dataframe[table].columns:
        dataframe[table].rename(columns={col: col.lower()}, inplace=True)

def create_fact_table(query_input):
    
    dataframe_dict = collect_queries(query_input)
    fact_name = "Fact"

    tables_to_process = gather_tables(query_input)

    for table in tables_to_process:
        if table != fact_name:
            dataframe_dict[fact_name] = merge_dataframe(dataframe_dict, fact_name, table)
            drop_columns = [col for col in dataframe_dict[table].columns if col != f"{table}_id"]
            dataframe_dict[fact_name].drop(columns=drop_columns, axis=1, inplace=True)

    dataframe_dict['Fact'].drop(columns='Fact_id', axis=1, inplace=True)

    for table in tables_to_process:
        rename_table_columns(dataframe_dict, table)


    return dataframe_dict
