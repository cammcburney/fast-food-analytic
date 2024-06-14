import pytest
import pandas as pd
from sqlalchemy import Engine
from sqlalchemy.testing.assertions import AssertsExecutionResults
from unittest.mock import MagicMock, patch
from src.utils.ingestion_utils import (
    read_data_file_into_dataframe,
    insert_data_into_database,
)


class Test_Read_Data_File_Into_Dataframe:

    def test_returns_type_dataframe(self):
        csv_file_path = "data/processed/cleaned-fast-food-data.csv"
        dataframe = read_data_file_into_dataframe(csv_file_path)

        assert type(dataframe) == pd.DataFrame

    def test_bad_path_raises_error(self):
        file_path = "data/processed/fast-food-data-sample.txt"
        with pytest.raises(Exception):
            read_data_file_into_dataframe(file_path)


class TestInsertDataIntoDatabase:

    def test_mock_insert_data_rows_into_mock_db(self):
        mock_engine = MagicMock()

        mock_dataframe = MagicMock(spec=pd.DataFrame)

        mock_to_sql_response = "Successfully inserted table"
        mock_dataframe.to_sql.return_value = mock_to_sql_response

        response = insert_data_into_database(
            mock_dataframe, mock_engine, table_name="mock"
        )

        mock_dataframe.to_sql.assert_called_once_with(
            "mock", mock_engine, if_exists="replace", index=False
        )

        assert response == "Successfully inserted table"

    def test_failure_to_insert_data_raises_error(self):
        bad_engine = ""
        table_name = "table"
        dataframe = ""
        with pytest.raises(Exception):
            insert_data_into_database(dataframe, bad_engine, table_name)
