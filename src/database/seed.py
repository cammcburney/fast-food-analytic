from src.utils.connection_utils import get_db_credentials, create_engine_connection
from src.utils.load_utils import run_engine_to_insert_to_database
from src.utils.ingestion_utils import (
    read_data_file_into_dataframe,
    insert_data_into_database,
)
from src.utils.processing_utils import create_star_schema_dict
from datetime import datetime

def seed_oltp(users, switch=False):
    csv_file_path = "data/processed/cleaned-fast-food-data.csv"

    credentials = get_db_credentials(users)
    engine = create_engine_connection(credentials,switch=switch)
    sample_dataframe = read_data_file_into_dataframe(csv_file_path)
    insert_data_into_database(engine=engine, dataframe=sample_dataframe, table_name="fast_food")
    engine.dispose()

def seed_olap_warehouse(df_dict, user):
    credentials_warehouse = get_db_credentials(user)
    engine = create_engine_connection(credentials_warehouse, switch=True)
    run_engine_to_insert_to_database(engine,df_dict)
    engine.dispose()

input_data = {
            "credentials": "user",
            "database_name": "fast_food",
            "queries": {
                        "manager": ["manager", "country", "city"],
                        "product": ["product", "price", "cost", "profit_unit"],
                        "purchase_type": ["purchase_type"],
                        "payment_method": ["payment_method"],
                        "fact": ["order_id", "date", "product", "price", "quantity", "cost", "profit_unit", "city", "country", "manager", "purchase_type", "payment_method", "revenue", "profit"]
                        }
            }

dt1 = datetime.now()
seed_oltp("user")
seed_oltp("test")

tables = create_star_schema_dict(input_data)

seed_olap_warehouse(tables, "user")
seed_olap_warehouse(tables, "test")
dt2 = datetime.now()

print(f"Tasks completed in {dt2 - dt1} seconds.")

