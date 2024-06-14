import pytest
import os
from unittest.mock import patch, Mock
from sqlalchemy import Engine
from src.utils.connection_utils import get_db_credentials, create_engine_connection

mock_credentials = {
    "user": "test",
    "password": "test!",
    "database": "oltpdatabase",
    "warehouse": "olapdatabase",
    "host": "localhost",
    "port": "5432",
}

mock_test_credentials = {
    "user": "test",
    "password": "test!",
    "database": "oltpdatabase",
    "host": "localhost",
    "port": "5432",
}


class TestGetDBConnection:

    def test_returns_dictionary(self):
        assert type(get_db_credentials("user")) == dict

    @patch(
        "src.utils.connection_utils.get_db_credentials", return_value=mock_credentials
    )
    def test_returns_user_credentials(self, mock_get_db_credentials):

        dummy_creds = mock_get_db_credentials("user")

        assert dummy_creds["port"] == "5432"
        assert dummy_creds["host"] == "localhost"
        assert dummy_creds["user"] == "test"
        assert dummy_creds["password"] == "test!"
        assert dummy_creds["database"] == "oltpdatabase"
        assert dummy_creds["warehouse"] == "olapdatabase"
        assert len(dummy_creds) == 6

    @patch(
        "src.utils.connection_utils.get_db_credentials",
        side_effect=Exception("Unable to reach requested environment variables."),
    )
    def test_exception_raised(self, mock_get_db_credentials):
        with pytest.raises(
            Exception, match="Unable to reach requested environment variables."
        ):
            mock_get_db_credentials("user")

    def test_invalid_credentials_type(self):
        with pytest.raises(
            ValueError,
            match="Invalid value for which_credentials. Expected 'user' or 'test'.",
        ):
            get_db_credentials("invalid")

    @patch(
        "src.utils.connection_utils.os.getenv",
        side_effect=lambda x: {
            "db_user": "test_user",
            "db_password": "test_password",
            "db_host": "test_host",
            "db_port": "test_port",
            "test_db_name": "test_database",
            "test_wh_name": "test_warehouse"
        }[x],
    )
    def test_get_user_credentials_test_path(self, mock_getenv):
        expected_credentials = {
            "user": "test_user",
            "password": "test_password",
            "host": "test_host",
            "port": "test_port",
            "database": "test_database",
            "warehouse": "test_warehouse"
        }

        actual_credentials = get_db_credentials("test")
        assert actual_credentials == expected_credentials

    @patch("src.utils.connection_utils.os.getenv", side_effect={})
    def test_raise_exception_no_variables(self, mock_getenv):
        with pytest.raises(Exception):
            get_db_credentials("user")


class Test_Create_Engine_Connection:

    @patch("src.utils.connection_utils.create_engine")
    def test_returns_type_engine(self, mock_create_engine):
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        engine = create_engine_connection(mock_credentials)
        assert isinstance(engine, Engine)

    @patch("src.utils.connection_utils.create_engine")
    def test_returns_type_engine_switch_on(self, mock_create_engine):
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        engine = create_engine_connection(mock_credentials, switch=True)
        assert isinstance(engine, Engine)

    @patch(
        "src.utils.connection_utils.create_engine_connection",
        side_effect=ConnectionError(
            "Error occured when connecting to the database, please check credentials."
        ),
    )
    def test_exception_raised(self, mock_create_engine_connection):
        with pytest.raises(
            ConnectionError,
            match="Error occured when connecting to the database, please check credentials.",
        ):
            mock_create_engine_connection(mock_credentials)

    def test_raise_exception_bad_credentials(self):
        bad_input = {}
        with pytest.raises(Exception):
            create_engine_connection(bad_input)
