def run_engine_to_insert_to_database(engine, input_dict):

    """
    Connects to database and appends rows given from a dataframe stored in a dictionary.

        Parameters:
            engine: A valid connection to a PSQL database using SQLAlchemy/pg8000.

            input_dict = A dictionary with keys of strings and values of dataframes.

        Returns:
            None

    """

    try:
        with engine.begin() as connection:
                for dataframe_name, dataframe in input_dict.items():
                    dataframe.to_sql(name=dataframe_name, con=connection, if_exists='append', index=False)
    except Exception:
        raise ValueError(
            "Failed to insert rows into database, check dataframe and engine inputs"
        )
        