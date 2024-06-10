from connection_utils import get_db_credentials, create_engine_connection
import pandas as pd
from sqlalchemy import text

input_data = {
            "credentials": "user",
            "database_name": "fast_food",
            "queries": {
                        "Manager": ["Manager", "Country", "City"],
                        "Product": ["Product", "Price", "Cost", "Profit/Unit"],
                        "Purchase_Type": ["Purchase Type"],
                        "Payment": ["Payment Method"],
                        "Fact": ["Order ID", "Date", "Product", "Price","Quantity", "Cost", "Profit/Unit", "City", "Country", "Manager", "Purchase Type", "Payment Method", "Revenue", "Profit"]
                        }
            }

def process_query_with_engine(engine, query):
    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()
    return result, rows

def query_function(credentials="user",
                   database_name=None, 
                   select_columns=None,
                   id_column=None
                   ):

    columns_str = ", ".join([f'"{column}"' for column in select_columns])
    creds = get_db_credentials(f"{credentials}")
    query = f'SELECT DISTINCT {columns_str} FROM {database_name};'
    engine = create_engine_connection(creds, switch=False)

    result, rows = process_query_with_engine(engine, query)

    dataframe = pd.DataFrame(rows, columns=result.keys())
    
    dataframe.insert(0, f"{id_column}_id", range(1, 1 + len(dataframe)))
    
    return dataframe

def collect_queries(query_input):

    credentials = query_input["credentials"]
    database_name = query_input["database_name"]
    queries = query_input["queries"]
    
    dataframe_dict = {}

    for id_name, query_list in queries.items():
        dataframe = query_function(credentials, database_name, query_list, id_name)
        dataframe_dict[id_name] = dataframe

    return dataframe_dict

def rename_column(processed_dataframe, merge_on):
    processed_dataframe.rename(columns={
            f"{merge_on}_id": f"{merge_on}_id.".upper(),
        }, inplace=True)
    
def merge_dataframe(dataframe_dict, to_merge, merge_on, how="left"):
    processed_df = dataframe_dict[to_merge]
    on = list(dataframe_dict[merge_on].columns[1:])
    suffixes = tuple(f"_{merge_on.lower()}")
    processed_df = processed_df.merge(dataframe_dict[merge_on], on=on, how=how, suffixes=suffixes)
    return processed_df

def create_fact_table(query_input):
    
    dataframe_dict = collect_queries(query_input)

    processed_dataframe = merge_dataframe(dataframe_dict, "Fact", "Manager")

    df = rename_column(processed_dataframe, "Manager")

    final_df = df[['Order ID', 'Date', 'Product_ID', 'Price','Quantity', 'Cost', 'Profit/Unit', 'Manager_ID',
                   'Country', 'Manager', 'Purchase_Type_ID', 'Payment_Method_ID', 'Revenue','Profit']]
    
    print(final_df)
    return final_df

create_fact_table(input_data)