import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

csv_file_path = 'data/processed/fast-food-data-sample.csv'

def read_data_file_into_dataframe(file_path):

    if file_path.endswith(".csv"):
        dataframe = pd.read_csv(file_path)

        return dataframe

def insert_data_into_database(dataframe, engine):

    table_name = 'fast_food'
    response = dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
    
    return response


