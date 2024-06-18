from src.utils.connection_utils import get_db_credentials, create_engine_connection
import pandas as pd
from pandas.errors import MergeError
from sqlalchemy import text
from src.utils.custom_errors import QueryExecutionError
from sqlalchemy.exc import SQLAlchemyError, DBAPIError, ProgrammingError

def process_query_with_engine(engine, query):

    """
    Connects to a database and runs a query returning the results.

        Parameters:
            engine: A valid connection to a PSQL database using SQLAlchemy/pg8000.
            query: A SQL query as a string.
            
        Returns:
            result: keys for rows.
            rows: row data as a list.

    """

    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
        return result, rows
    except SQLAlchemyError:
        raise QueryExecutionError("Failed to to run query, check parameters and connection are valid")


def query_function(credentials="user",
                   database_name=None, 
                   select_columns=None,
                   id_column=None):
    
    """
    Connects to a database with chosen credentials, runs a query and returns the results as a dataframe.

        Parameters:
            credentials: string of 'user' or 'test' to gather credentials.
            database_name: string of database name to connect to.
            select_columns: column names to source rows data from.
            id_column: name of data table to create unique key from.
            
        Returns:
            dataframe of requested information.

    """

    try:
        if database_name is None or select_columns is None or id_column is None:
            raise ValueError("database name, columns, and id cannot be None.")

        columns_str = ", ".join([f'"{column}"' for column in select_columns])
        creds = get_db_credentials(f"{credentials}")
        query = f'SELECT DISTINCT {columns_str} FROM {database_name};'
        engine = create_engine_connection(creds, switch=False)

        result, rows = process_query_with_engine(engine, query)

        dataframe = pd.DataFrame(rows, columns=result.keys())
        
        dataframe.insert(0, f"{id_column}_id", range(1, 1 + len(dataframe)))
        
        return dataframe
    
    except (SQLAlchemyError, DBAPIError, ProgrammingError):
        raise QueryExecutionError("Failed to to run query, check parameters and connection are valid")
    except ValueError:
        raise ValueError("Incorrect values given, please check inputs.")
    
def collect_queries(query_input):

    """
    Collects dataframes by querying the database and stores them in a dictionary.

        Parameters:
            query_input: takes a dictionary format of the following data, can take any number of queries,
            all queries must be stored in dictionary format with their respective table name as the key.
                "credentials": "example_user",
                "database_name": "example_db_name",
                "queries": {
                        "example_table_name": ["column1", "column2", "column3"],
                        "example_table_name": ["column1", "column2", "column3", "column4"]
                        }
            }
            
        Returns:
            a dictionary of dataframes with keys as strings and values as dataframes.
    """

    try:
        credentials = query_input["credentials"]
        database_name = query_input["database_name"]
        queries = query_input["queries"]
        
        dataframe_dict = {}

        for id_name, query_list in queries.items():
            dataframe = query_function(credentials, database_name, query_list, id_name)
            dataframe_dict[id_name] = dataframe

        return dataframe_dict
    except (SQLAlchemyError, DBAPIError, ProgrammingError):
        raise QueryExecutionError("Failed to to run query, check parameters and connection are valid")
    except KeyError:
        raise KeyError("Incorrect values given, please check inputs.")
    
def merge_dataframe(dataframe_dict, to_merge, merge_on, how="left"):

    """
    Merges dataframes for a star schema by creating id columns to link them.

        Parameters:
            dataframe_dict: a dictionary of dataframes with their key (table name) as a string
                            and the value as a dataframe.
            to_merge: the table to merge.
            merge_on: the table to merge onto.
            how: how to join the tables, defaults to left.
            
        Returns:
            a merged dataframe with the requested id keys in a column.

    """

    valid_how_values = {"left", "right", "outer", "inner", "cross"}
    
    if how not in valid_how_values:
        raise ValueError("Incorrect values given, please check inputs.")
    
    try:
        processed_df = dataframe_dict[to_merge]
        on = list(dataframe_dict[merge_on].columns[1:])
        suffixes = tuple(f"'', '_{merge_on}'")
        processed_df = processed_df.merge(dataframe_dict[merge_on], on=on, how=how, suffixes=suffixes)

        return processed_df
    except (KeyError, ValueError, MergeError, TypeError):
        raise Exception("Incorrect values given, please check inputs.")

def gather_tables(query_input):

    """
    Connects to a database and runs a query returning the results.

        Parameters:
            query_input: takes a dictionary format of the following data, can take any number of queries,
            all queries must be stored in dictionary format with their respective table name as the key.
                "credentials": "example_user",
                "database_name": "example_db_name",
                "queries": {
                        "example_table_name": ["column1", "column2", "column3"],
                        "example_table_name": ["column1", "column2", "column3", "column4"]
                        }
            }
            
        Returns:
            returns the table names from queries as a list.

    """

    try:
        tables_to_process = []
        for key, _ in query_input["queries"].items():
            tables_to_process.append(key)

        return tables_to_process
    except (KeyError):
        raise KeyError("Table names not found, check input")

def create_star_schema_dict(query_input):

    """
    Processes the query to create a dictionary of dataframes and then cleans them to be optimised for querying.

        Parameters:
            query_input: takes a dictionary format of the following data, can take any number of queries,
            all queries must be stored in dictionary format with their respective table name as the key.
                "credentials": "example_user",
                "database_name": "example_db_name",
                "queries": {
                        "example_table_name": ["column1", "column2", "column3"],
                        "example_table_name": ["column1", "column2", "column3", "column4"]
                        }
            }
            
        Returns:
            returns a dictionary of dataframes set up for a star schema format.

    """
    
    try:
        dataframe_dict = collect_queries(query_input)
        
        fact_name = "fact"

        tables_to_process = gather_tables(query_input)

        for table in tables_to_process:
            if table != fact_name:
                dataframe_dict[fact_name] = merge_dataframe(dataframe_dict, fact_name, table)
                drop_columns = [col for col in dataframe_dict[table].columns if col != f"{table}_id"]
                dataframe_dict[fact_name].drop(columns=drop_columns, axis=1, inplace=True)

        dataframe_dict['fact'].drop(columns='fact_id', axis=1, inplace=True)
        dataframe_dict['product'].drop(columns='product_id', axis=1, inplace=True)
        dataframe_dict['manager'].drop(columns='manager_id', axis=1, inplace=True)
        dataframe_dict['purchase_type'].drop(columns='purchase_type_id', axis=1, inplace=True)
        dataframe_dict['payment_method'].drop(columns='payment_method_id', axis=1, inplace=True)

        return dataframe_dict
    
    except (SQLAlchemyError, DBAPIError, ProgrammingError):
        raise QueryExecutionError("Failed to to run query, check parameters and connection are valid")
    except (KeyError, ValueError):
        raise KeyError("Incorrect values given, please check inputs.")
