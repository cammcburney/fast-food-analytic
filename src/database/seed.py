from src.utils.connection_utils import get_db_credentials, create_engine_connection
from src.utils.ingestion_utils import (
    read_data_file_into_dataframe,
    insert_data_into_database,
)

csv_file_path = "data/processed/fast-food-data-sample.csv"

credentials = get_db_credentials("user")
engine = create_engine_connection(credentials)
sample_dataframe = read_data_file_into_dataframe(csv_file_path)
insert_data_into_database(engine=engine, dataframe=sample_dataframe, table_name="fast_food")