import pg8000
from src.utils.connection_utils import get_db_credentials,create_engine_connection
from src.utils.processing_utils import create_fact_table
def create_dim_tables():
    create_dim_manager = """
    CREATE TABLE IF NOT EXISTS manager (
        "manager_id" SERIAL PRIMARY KEY NOT NULL,
        "manager" VARCHAR(50) NOT NULL,
        "country" VARCHAR(50) NOT NULL,
        "city" VARCHAR(50) NOT NULL
    );
    """
    
    create_dim_product = """
    CREATE TABLE IF NOT EXISTS product (
        "product_id" SERIAL PRIMARY KEY NOT NULL,
        "product" VARCHAR(100) NOT NULL,
        "price" FLOAT,
        "cost" DECIMAL NOT NULL,
        "profit_unit" DECIMAL NOT NULL
    );
    """

    create_dim_purchase_type = """
    CREATE TABLE IF NOT EXISTS purchase_type (
        "purchase_type_id" SERIAL PRIMARY KEY NOT NULL,
        "purchase_type" VARCHAR(50) NOT NULL
    );
    """
    
    create_dim_payment_method = """
    CREATE TABLE IF NOT EXISTS payment_method (
        "payment_method_id" SERIAL PRIMARY KEY NOT NULL,
        "payment_method" VARCHAR(50) NOT NULL
    );
    """
    
    return create_dim_manager, create_dim_product, create_dim_purchase_type, create_dim_payment_method

def create_fact_warehouse():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fact (
        "order_id" SERIAL PRIMARY KEY NOT NULL,
        "date" VARCHAR,
        "product_id" INT NOT NULL,
        "price" FLOAT,
        "quantity" INT NOT NULL,
        "cost" FLOAT,
        "profit_unit" FLOAT,
        "manager_id" INT NOT NULL,
        "purchase_type_id" INT NOT NULL,
        "payment_method_id" INT NOT NULL,
        "revenue" DECIMAL NOT NULL,
        "profit" DECIMAL NOT NULL,
        FOREIGN KEY ("manager_id") REFERENCES manager("manager_id"),
        FOREIGN KEY ("product_id") REFERENCES product("product_id"),
        FOREIGN KEY ("purchase_type_id") REFERENCES purchase_type("purchase_type_id"),
        FOREIGN KEY ("payment_method_id") REFERENCES payment_method("payment_method_id")
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
            database=credentials_warehouse["warehouse"]
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



create_tables("user")