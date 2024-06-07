from src.python.connection_utils import (get_db_connection,
                                        create_engine_connection)
from src.python.ingestion_utils import (read_data_file_into_dataframe,
                                        insert_data_into_database)

csv_file_path = 'data/processed/fast-food-data-sample.csv'

test_credentials = get_db_connection()