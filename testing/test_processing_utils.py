import pytest
import pandas as pd
from sqlalchemy.engine import CursorResult
from src.utils.connection_utils import create_engine_connection, get_db_credentials
from src.utils.processing_utils import (process_query_with_engine,
                                        query_function,
                                        collect_queries,
                                        merge_dataframe,
                                        gather_tables,

                                        create_star_schema_dict)
from src.utils.custom_errors import QueryExecutionError

test_input_data = {
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

class Test_Run_Query_With_Engine:
    def test_results_result_and_rows(self):
        query = "SELECT * FROM fast_food LIMIT 5"
        db_credentials = get_db_credentials("test")
        engine = create_engine_connection(db_credentials, switch=False)

        result, rows = process_query_with_engine(engine, query)
        assert isinstance(result, CursorResult)
        
        
        assert len(rows) == 5
        assert rows[0][0] == 10454
        assert rows[0][1] == '07/11/2022'
        assert rows[0][2] == 'Sides & Other'
        assert rows[0][3] == 4.99
    
    def test_connection_error_raised_bad_query(self):
        db_credentials = get_db_credentials("test")
        engine = create_engine_connection(db_credentials, switch=False)
        bad_input = "SELECT * FROM faketable"

        with pytest.raises(QueryExecutionError) as e:
            process_query_with_engine(engine, bad_input)
        assert str(e.value) == "Failed to to run query, check parameters and connection are valid"

class Test_Query_Function:
    def test_returns_dataframe(self):
        credentials = "test"
        database_name = test_input_data["database_name"]
        queries = test_input_data["queries"]
        value = queries["manager"]
        
        dataframe = query_function(credentials, database_name, value, "manager")

        assert isinstance(dataframe, pd.DataFrame)
        
        assert dataframe["manager"][1] == "Pablo Perez"
        assert dataframe["country"][0] == "France"
        assert dataframe["city"][4] == "Lisbon"

    def test_bad_query_raises_sql_error(self):
        input = {"credentials": "user",
                    "database_name": "fast_food",
                    "queries": {
                                "manager": ["manager", "wall", "city"],
                                }
                }
                
        with pytest.raises(QueryExecutionError) as e:
            query_function("test", input["database_name"], input["queries"]["manager"], "manager")
        assert str(e.value) == "Failed to to run query, check parameters and connection are valid"

    def test_bad_input_raises_value_error(self):
        input = {"credentials": "user",
                    "database_name": "fast_food"  
                }
        
        with pytest.raises(ValueError) as e:
            query_function("test", input["database_name"], "manager")
        assert str(e.value) == "Incorrect values given, please check inputs."

class Test_Collect_Queries:
    def test_returns_dictionary_of_dataframes(self):
        df_dictionary = collect_queries(test_input_data)

        assert isinstance(df_dictionary, dict)
        for key, value in df_dictionary.items():
            assert isinstance(key, str)
            assert isinstance(value, pd.DataFrame)

    def test_bad_query_raises_sql_error(self):
        input = {"credentials": "user",
                    "database_name": "fard_food",
                    "queries": {
                                "manager": ["manager", "wall", "city"],
                                }
                }
                
        with pytest.raises(QueryExecutionError) as e:
            collect_queries(input)
        assert str(e.value) == "Failed to to run query, check parameters and connection are valid"

    def test_bad_input_raises_key_error(self):
        input = {"credentials": "user"}

        with pytest.raises(KeyError) as e:
            collect_queries(input)
        assert str(e.value) == "'Incorrect values given, please check inputs.'"

class Test_Merge_Dataframe:
    def test_merge_dataframes_creates_keys(self):
        dataframe_dict = collect_queries(test_input_data)

        merged_fact_dict = merge_dataframe(dataframe_dict, "fact", "manager")
        assert "manager_id" in merged_fact_dict.columns
        assert len(merged_fact_dict.columns) > len(dataframe_dict["fact"].columns)
    
    def test_bad_input_raises_key_error(self):
        dataframe_dict = collect_queries(test_input_data)

        with pytest.raises(Exception) as e:
            merge_dataframe(dataframe_dict, "fact", "wall")
        assert str(e.value) == "Incorrect values given, please check inputs."

        with pytest.raises(Exception) as e:
            merge_dataframe(dataframe_dict, "fact", "manager", how="up")
        assert str(e.value) == "Incorrect values given, please check inputs."


class Test_Gather_Tables:
    def test_gathers_tables(self):
        tables = gather_tables(test_input_data)
        assert isinstance(tables, list)
        assert tables == ['manager', 'product', 'purchase_type', 'payment_method', 'fact']

    def test_bad_input_raises_key_error(self):
        input = {"credentials": "user"}

        with pytest.raises(KeyError) as e:
            gather_tables(input)
        assert str(e.value) == "'Table names not found, check input'"

class Test_Create_Star_Schema_Dict:
    def test_returns_cleaned_dictionary_of_dataframes(self):
        comparison_dict = collect_queries(test_input_data)
        star_schema_dict = create_star_schema_dict(test_input_data)

        assert len(star_schema_dict["manager"].columns) != len(comparison_dict["manager"].columns)
        assert len(star_schema_dict["product"].columns) != len(comparison_dict["product"].columns)
        assert len(star_schema_dict["fact"].columns) != len(comparison_dict["fact"].columns)
        assert len(star_schema_dict["payment_method"].columns) != len(comparison_dict["payment_method"].columns)
        assert len(star_schema_dict["purchase_type"].columns) != len(comparison_dict["purchase_type"].columns)

        assert isinstance(star_schema_dict, dict)
        for key, value in star_schema_dict.items():
            assert isinstance(key, str)
            assert isinstance(value, pd.DataFrame)

    def test_bad_query_raises_sql_error(self):
        input = {"credentials": "user",
                    "database_name": "fard_food",
                    "queries": {
                                "manager": ["manager", "wall", "city"],
                                }
                }
                
        with pytest.raises(QueryExecutionError) as e:
            create_star_schema_dict(input)
        assert str(e.value) == "Failed to to run query, check parameters and connection are valid"

    def test_bad_input_raises_key_error(self):
        input = {"credentials": "user"}

        with pytest.raises(KeyError) as e:
            create_star_schema_dict(input)
        assert str(e.value) == "'Incorrect values given, please check inputs.'"
