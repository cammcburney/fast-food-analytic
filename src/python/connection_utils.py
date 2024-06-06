import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

def get_db_connection():

    load_dotenv(override=True)

    connection_credentials = {}
    connection_credentials["user"] = os.getenv("db_user")
    connection_credentials["password"] = os.getenv("db_password")
    connection_credentials["database"] = os.getenv("db_name")
    connection_credentials["host"] = os.getenv("db_host")
    connection_credentials["port"] = os.getenv("db_port")

    return connection_credentials

def create_engine_connection(connection_credentials):

    db_user = connection_credentials["user"]
    db_password = connection_credentials["password"]
    db_host = connection_credentials["host"]
    db_port = connection_credentials["port"]
    db_name = connection_credentials["database"]

    engine = create_engine(f'postgresql+pg8000://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    return engine

