def run_engine_to_insert_database(engine, input_dict):
    print(input_dict)
    with engine.begin() as connection:
            for dataframe_name, dataframe in input_dict.items():
                print(dataframe)
                dataframe.to_sql(name=dataframe_name, con=connection, if_exists='append', index=False)
                success_message = "Succesfully moved dataframe rows to SQL database"
            return success_message