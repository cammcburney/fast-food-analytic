import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

def get_db_credentials(which_credentials):

    """
    Gets a dictionary of database credentials from environment variables.
    
    Parameters:
        which_credentials: either "user" or "test" must be inputted to get
                          the requested set of credentials.
                          
    Returns:
        dictionary of database credentials
    """

    load_dotenv(override=True)

    connection_credentials = {}
    
    if which_credentials not in ["user", "test"]:
        raise ValueError("Invalid value for which_credentials. Expected 'user' or 'test'.")

    try:
        connection_credentials["user"] = os.getenv("db_user")
        connection_credentials["password"] = os.getenv("db_password")
        connection_credentials["host"] = os.getenv("db_host")
        connection_credentials["port"] = os.getenv("db_port")
        
        if which_credentials == "user":
            connection_credentials["database"] = os.getenv("db_name")
            connection_credentials["warehouse"] = os.getenv("db_wname")
        elif which_credentials == "test":
            connection_credentials["database"] = os.getenv("test_db_name")
                
        return connection_credentials
    
    except Exception:
        raise Exception("Unable to reach requested environment variables.")

def create_engine_connection(connection_credentials, switch=False):

    """
    Creates a connection to the database.

        Parameters:
            connection_credentials: takes a dictionary of database credentials

            switch: if False (Default), the database credentials are used.
                    if True the warehouse credentials are used

        Returns:
            engine connection to given database

    """

    try:
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
    
    except Exception:
        raise ConnectionError("Error occured when connecting to the database, please check credentials.")