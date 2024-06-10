import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv


def read_data_file_into_dataframe(file_path):
    """
    Converts file contents into a dataframe.

        Parameters:
            file_path: local file path to data

        Returns:
            dataframe of given file
    """

    if file_path.endswith(".csv"):
        dataframe = pd.read_csv(file_path)

        return dataframe
    else:
        raise Exception("Invalid file format, please check given path")


def insert_data_into_database(dataframe, engine, table_name):
    """
    Creates table for given dataframe and inserts it into the database, overwrites if exists.

        Parameters:
            dataframe:
            engine: working sqlalchemy engine connection
            table_name: string of given table name

        Returns:
            dataframe of given file
    """
    try:
        response = dataframe.to_sql(
            table_name, engine, if_exists="append", index=False
        )

        return response
    except Exception:
        raise Exception(
            "Failed to insert table into database, check dataframe and engine inputs"
        )
