from src.utils.connection_utils import run_engine_to_insert_database,get_db_credentials, create_engine_connection
from src.utils.ingestion_utils import (
    read_data_file_into_dataframe,
    insert_data_into_database,
)
from src.utils.processing_utils import create_fact_table
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

input_data = {
            "credentials": "user",
            "database_name": "fast_food",
            "queries": {
                        "Manager": ["Manager", "Country", "City"],
                        "Product": ["Product", "Price", "Cost", "Profit/Unit"],
                        "Purchase_Type": ["Purchase Type"],
                        "Payment_Method": ["Payment Method"],
                        "Fact": ["Order ID", "Date", "Product", "Price","Quantity", "Cost", "Profit/Unit", "City", "Country", "Manager", "Purchase Type", "Payment Method", "Revenue", "Profit"]
                        }
            }

test=warehouse(create_fact_table(input_data))
    