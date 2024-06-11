import pg8000
from src.utils.connection_utils import get_db_credentials
def create_dim_tables():
    create_dim_manager = """
    CREATE TABLE IF NOT EXISTS Dim_Manager (
        "Manager_ID" INT PRIMARY KEY,
        "Manager" VARCHAR(50),
        "Country" VARCHAR(50),
        "City" VARCHAR(50)
    );
    """
    
    create_dim_product = """
    CREATE TABLE IF NOT EXISTS Dim_Product (
        "Product_ID" INT PRIMARY KEY,
        "Product" VARCHAR(100),
        "Price" FLOAT,
        "Cost" FLOAT,
        "Profit/Unit" FLOAT
    );
    """

    create_dim_purchase_type = """
    CREATE TABLE IF NOT EXISTS Dim_Purchase_Type (
        "Purchase_Type_ID" INT PRIMARY KEY,
        "Purchase_Type" VARCHAR(50)
    );
    """
    
    create_dim_payment_method = """
    CREATE TABLE IF NOT EXISTS Dim_Payment_Method (
        "Payment_Method_ID" INT PRIMARY KEY,
        "Payment_Method" VARCHAR(50)
    );
    """
    
    return create_dim_manager, create_dim_product, create_dim_purchase_type, create_dim_payment_method

def create_fact_warehouse():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fact_sales (
        "Order_ID" INT PRIMARY KEY,
        "Date" DATE,
        "Product_ID" INT,
        "Price" FLOAT,
        "Quantity" INT,
        "Cost" FLOAT,
        "Profit/Unit" FLOAT,
        "Manager_ID" INT,
        "Purchase_Type_ID" INT,
        "Payment_Method_ID" INT,
        "Revenue" FLOAT,
        "Profit" FLOAT,
        FOREIGN KEY ("Manager_ID") REFERENCES Dim_Manager("Manager_ID"),
        FOREIGN KEY ("Product_ID") REFERENCES Dim_Product("Product_ID"),
        FOREIGN KEY ("Purchase_Type_ID") REFERENCES Dim_Purchase_Type("Purchase_Type_ID"),
        FOREIGN KEY ("Payment_Method_ID") REFERENCES Dim_Payment_Method("Payment_Method_ID")
    );
    """
    return create_table_sql


def create_tables():
    credentials_warehouse = get_db_credentials("user")

    try:
        connection = pg8000.connect(
            user=credentials_warehouse["user"],
            password=credentials_warehouse["password"],
            host=credentials_warehouse["host"],
            port=int(credentials_warehouse["port"]),
            database=credentials_warehouse["database"]
        )
        
        cursor = connection.cursor()
        
        dim_tables_sql = create_dim_tables()
        for sql in dim_tables_sql:
            cursor.execute(sql)

        fact_table_sql = create_fact_warehouse()
        cursor.execute(fact_table_sql)
        
        connection.commit()
        
        print("All tables created successfully.")
    
    except Exception as e:
        print("An error occurred while creating the tables:", e)
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


create_tables()