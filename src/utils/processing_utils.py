from connection_utils import get_db_credentials, create_engine_connection
import pandas as pd
from sqlalchemy import text
def manager_query():
    creds = get_db_credentials("user")
    query = 'SELECT DISTINCT "Manager", "Country", "City" FROM fast_food;'
    engine = create_engine_connection(creds, switch=False)

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()
        
    
    df = pd.DataFrame(rows, columns=result.keys())
    
    
    df.insert(0, 'manager_id', range(1, 1 + len(df)))
    
    print(df)
    return df

def product_query():
    creds = get_db_credentials("user")
    query = 'SELECT DISTINCT "Product", "Price", "Cost", "Profit/Unit" FROM fast_food;'
    engine = create_engine_connection(creds, switch=False)

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()
        
    
    df = pd.DataFrame(rows, columns=result.keys())
    
    
    df.insert(0, 'product_id', range(1, 1 + len(df)))
    
    print(df)
    return df

def purchase_query():
    creds = get_db_credentials("user")
    query = 'SELECT DISTINCT "Purchase Type" FROM fast_food;'
    engine = create_engine_connection(creds, switch=False)

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()
        
    
    df = pd.DataFrame(rows, columns=result.keys())
    
    
    df.insert(0, 'purchase_type_id', range(1, 1 + len(df)))
    
    print(df)
    return df

def payment_method_query():
    creds = get_db_credentials("user")
    query = 'SELECT DISTINCT "Payment Method" FROM fast_food;'
    engine = create_engine_connection(creds, switch=False)

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()
        
    
    df = pd.DataFrame(rows, columns=result.keys())
    
    
    df.insert(0, 'payment_id', range(1, 1 + len(df)))
    
    print(df)
    return df

managers_df = manager_query()
products_df = product_query()
purchase_types_df = purchase_query()
payment_methods_df = payment_method_query()

def combined_data_query():
    creds = get_db_credentials("user")
    query = '''
    SELECT "Order ID", "Date", "Product", "Price","Quantity", "Cost", "Profit/Unit",
           "City", "Country", "Manager", "Purchase Type", "Payment Method", "Revenue", "Profit"
    FROM fast_food;
    '''
    engine = create_engine_connection(creds, switch=False)

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()

   
    df = pd.DataFrame(rows, columns=result.keys())
    
    
    df = df.merge(managers_df, on=["City", "Country", "Manager"], how='left', suffixes=('', '_manager'))
    
    
    df = df.merge(products_df, on=["Product", "Price", "Cost", "Profit/Unit"], how='left', suffixes=('', '_product'))
    
    
    df = df.merge(purchase_types_df, on=["Purchase Type"], how='left', suffixes=('', '_purchase_type'))
    
   
    df = df.merge(payment_methods_df, on=["Payment Method"], how='left', suffixes=('', '_payment_method'))

   
    df.rename(columns={
        'manager_id': 'manager_id',
        'product_id': 'product_id',
        'purchase_type_id': 'purchase_type_id',
        'payment_id': 'payment_method_id'
    }, inplace=True)

  
    final_df = df[['Order ID', 'Date', 'product_id', 'Price','Quantity', 'Cost', 'Profit/Unit', 'manager_id',
                   'Country', 'Manager', 'purchase_type_id', 'payment_method_id', 'Revenue','Profit']]
    
    print(final_df)
    return final_df


test = combined_data_query()