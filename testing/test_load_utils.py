import pytest
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.testing.assertions import AssertsExecutionResults
from unittest.mock import MagicMock, patch
from decimal import Decimal
from src.utils.load_utils import run_engine_to_insert_database
from src.utils.connection_utils import create_engine_connection, get_db_credentials
from src.utils.processing_utils import create_star_schema_dict

test_input = {
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

class TestEngineInsertData:
    def test_run_engine_to_insert_database(self):

        db_credentials = get_db_credentials("test")
        engine = create_engine_connection(db_credentials, switch=True)
        dataframe_dictionary = create_star_schema_dict(test_input)

        with engine.connect() as connection:
            connection.execute(text("TRUNCATE fact CASCADE"))
            connection.commit()
        run_engine_to_insert_database(engine, dataframe_dictionary)
        
        with engine.connect() as connection:
            result1 = connection.execute(text("SELECT * FROM product ORDER BY product_id")).fetchall()
        
        columns = ["product_id", "product", "price", "cost", "profit_unit"]
        result1_df = pd.DataFrame(result1, columns=columns)
        
        assert result1_df["price"][4] == 12.99
        assert result1_df["cost"][1] == Decimal("0.47")

    def test_sad_path_raises_value_error(self):
        db_credentials = get_db_credentials("test")
        engine = create_engine_connection(db_credentials, switch=True)
        bad_input = {"terrible": ["bad"]}

        with pytest.raises(ValueError):
            run_engine_to_insert_database(engine, bad_input)

        
        