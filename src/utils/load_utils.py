import pandas

def run_engine_to_insert_database(engine, input_dict):
    try:
        with engine.begin() as connection:
                for dataframe_name, dataframe in input_dict.items():
                    dataframe.to_sql(name=dataframe_name, con=connection, if_exists='append', index=False)
    except Exception:
        raise ValueError(
            "Failed to insert rows into database, check dataframe and engine inputs"
        )
        