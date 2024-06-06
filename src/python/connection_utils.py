import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

def get_db_connection():
    #make 2x dictionary for warehouse name
    load_dotenv(override=True)

    connection_credentials = {}
    connection_credentials["user"] = os.getenv("db_user")
    connection_credentials["password"] = os.getenv("db_password")
    connection_credentials["database"] = os.getenv("db_name")
    connection_credentials["warehouse"] = os.getenv("db_wname")
    connection_credentials["host"] = os.getenv("db_host")
    connection_credentials["port"] = os.getenv("db_port")

    return connection_credentials

def create_engine_connection(connection_credentials, switch=False):

    db_user = connection_credentials["user"]
    db_password = connection_credentials["password"]
    db_host = connection_credentials["host"]
    db_port = connection_credentials["port"]
    if switch == False:
        db_name = connection_credentials["database"]
    else: 
        db_name = connection_credentials["warehouse"]
        
    engine = create_engine(f'postgresql+pg8000://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    return engine

