import pg8000
from src.utils.connection_utils import get_db_credentials
from src.utils.processing_utils import create_fact_table
def create_dim_tables():
    create_dim_manager = """
    CREATE TABLE IF NOT EXISTS Manager (
        "manager_id" SERIAL PRIMARY KEY NOT NULL,
        "manager" VARCHAR(50) NOT NULL,
        "country" VARCHAR(50) NOT NULL,
        "city" VARCHAR(50) NOT NULL
    );
    """
    
    create_dim_product = """
    CREATE TABLE IF NOT EXISTS Product (
        "product_id" SERIAL PRIMARY KEY NOT NULL,
        "product" VARCHAR(100) NOT NULL,
        "price" FLOAT NOT NULL,
        "cost" FLOAT NOT NULL,
        "profit/unit" FLOAT NOT NULL
    );
    """

    create_dim_purchase_type = """
    CREATE TABLE IF NOT EXISTS Purchase_Type (
        "purchase_type_id" SERIAL PRIMARY KEY NOT NULL,
        "purchase_type" VARCHAR(50) NOT NULL
    );
    """
    
    create_dim_payment_method = """
    CREATE TABLE IF NOT EXISTS Payment_Method (
        "payment_method_id" SERIAL PRIMARY KEY NOT NULL,
        "payment_method" VARCHAR(50) NOT NULL
    );
    """
    
    return create_dim_manager, create_dim_product, create_dim_purchase_type, create_dim_payment_method

def create_fact_warehouse():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS Fact (
        "order_id" SERIAL PRIMARY KEY NOT NULL,
        "date" DATE NOT NULL,
        "product_id" INT NOT NULL,
        "price" FLOAT NOT NULL,
        "quantity" INT NOT NULL,
        "cost" FLOAT NOT NULL,
        "profit/unit" FLOAT NOT NULL,
        "manager_id" INT NOT NULL,
        "purchase_type_id" INT NOT NULL,
        "payment_method_id" INT NOT NULL,
        "revenue" FLOAT NOT NULL,
        "profit" FLOAT NOT NULL,
        FOREIGN KEY ("manager_id") REFERENCES Manager("manager_id"),
        FOREIGN KEY ("product_id") REFERENCES Product("product_id"),
        FOREIGN KEY ("purchase_type_id") REFERENCES Purchase_Type("purchase_type_id"),
        FOREIGN KEY ("payment_method_id") REFERENCES Payment_Method("payment_method_id")
    );
    """
    return create_table_sql


def create_tables(user):
    credentials_warehouse = get_db_credentials(user)

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



create_tables("test")