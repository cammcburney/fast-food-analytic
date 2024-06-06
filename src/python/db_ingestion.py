import pandas as pd
from sqlalchemy import create_engine

db_user = 'your_db_user'
db_password = 'your_db_password'
db_host = 'localhost'
db_port = '5432'
db_name = 'oltpdatabase'


engine = create_engine(f'postgresql+pg8000://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')


csv_file_path = 'path_to_your_file.csv'
df = pd.read_csv(csv_file_path)


table_name = 'fast_food'
df.to_sql(table_name, engine, if_exists='replace', index=False)
