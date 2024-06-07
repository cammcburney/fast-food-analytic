import pytest
import pandas as pd
from sqlalchemy import Engine
from sqlalchemy.testing.assertions import AssertsExecutionResults
from unittest.mock import MagicMock, patch
from src.utils.ingestion_utils import (read_data_file_into_dataframe,
                                     insert_data_into_database)

class Test_Read_Data_File_Into_Dataframe:

    def test_returns_type_dataframe(self):
        csv_file_path = 'data/processed/fast-food-data-sample.csv'
        dataframe = read_data_file_into_dataframe(csv_file_path)

        assert type(dataframe) == pd.DataFrame

class TestInsertDataIntoDatabase:

    def test_mock_insert_data_rows_into_mock_db(self):
        mock_engine = MagicMock()
    
        mock_dataframe = MagicMock(spec=pd.DataFrame)
        
        mock_to_sql_response = "Successfully inserted table"
        mock_dataframe.to_sql.return_value = mock_to_sql_response
    
        response = insert_data_into_database(mock_dataframe, mock_engine, table_name = "mock")

        mock_dataframe.to_sql.assert_called_once_with("mock", mock_engine, if_exists='replace', index=False)
        
        assert response == "Successfully inserted table"
        
     