from src.utils.connection_utils import run_engine_to_insert_database,get_db_credentials, create_engine_connection
from src.utils.ingestion_utils import (
    read_data_file_into_dataframe,
    insert_data_into_database,
)
from src.utils.processing_utils import create_facts_table
def ingestion():
    csv_file_path = "data/processed/fast-food-data-sample.csv"

    credentials = get_db_credentials("user")
    engine = create_engine_connection(credentials)
    sample_dataframe = read_data_file_into_dataframe(csv_file_path)
    insert_data_into_database(engine=engine, dataframe=sample_dataframe, table_name="fast_food")
    engine.dispose()

def warehouse(df_dict):
    credentials_warehouse = get_db_credentials("user")
    #print(credentials_warehouse)
    engine = create_engine_connection(credentials_warehouse, switch=True)
    print(engine)
    insert =run_engine_to_insert_database(engine,df_dict)
    print(insert)
    