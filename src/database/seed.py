from src.utils.connection_utils import get_db_credentials, create_engine_connection
from src.utils.load_utils import run_engine_to_insert_database
from src.utils.ingestion_utils import (
    read_data_file_into_dataframe,
    insert_data_into_database,
)
from src.utils.processing_utils import create_fact_table

def ingestion(dict, user):
    csv_file_path = "data/processed/cleaned-fast-food-data.csv"

    credentials = get_db_credentials("user")
    engine = create_engine_connection(credentials)
    sample_dataframe = read_data_file_into_dataframe(csv_file_path)
    insert_data_into_database(engine=engine, dataframe=sample_dataframe, table_name="fast_food")
    engine.dispose()

def ingest_data(df_dict, user):
    credentials_warehouse = get_db_credentials(user)
    engine = create_engine_connection(credentials_warehouse, switch=False)
    run_engine_to_insert_database(engine,df_dict)

def warehouse(df_dict, user):
    credentials_warehouse = get_db_credentials(user)
    engine = create_engine_connection(credentials_warehouse, switch=True)
    print(engine)
    insert =run_engine_to_insert_database(engine,df_dict)
    print(insert)

input_data = {
            "credentials": "user",
            "database_name": "fast_food",
            "queries": {
                        "Manager": ["Manager", "Country", "City"],
                        "Product": ["Product", "Price", "Cost", "Profit/Unit"],
                        "Purchase_Type": ["Purchase_Type"],
                        "Payment_Method": ["Payment_Method"],
                        "Fact": ["Order_ID", "Date", "Product", "Price", "Quantity", "Cost", "Profit/Unit", "City", "Country", "Manager", "Purchase_Type", "Payment_Method", "Revenue", "Profit"]
                        }
            }

tables = create_fact_table(input_data)

ingest_data(tables, "test")