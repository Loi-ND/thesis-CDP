import psycopg2
from typing import Any, Dict, List
from psycopg2.extensions import connection

def get_postgres_connection() -> connection:
    """
    Establishes a connection to the PostgreSQL database.

    Returns:
        psycopg2.extensions.connection: A connection object to the PostgreSQL database.
    """
    try:
        connection = psycopg2.connect(
            host="postgres",
            port=5432,
            user="postgres",
            password="postgres",
            dbname="project"
        )
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        raise

def import_initial_data_to_ops() -> None:
    """
    Imports initial data into the 'ops' table in the PostgreSQL database.
    """
    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("DROP SCHEMA IF EXISTS ops CASCADE;")

        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS ops;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ops.customers (
                customer_id BIGINT PRIMARY KEY,
                phone_number VARCHAR(20) NOT NULL,
                email VARCHAR(255)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ops.products (
                product_id BIGINT PRIMARY KEY,
                category_id BIGINT,
                category_code VARCHAR(50),
                brand VARCHAR(100),
                price DECIMAL(10, 2) NOT NULL
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ops.orders (
                customer_id BIGINT,
                product_id BIGINT,
                time VARCHAR(20) NOT NULL
            );
        """)

        conn.commit()
        conn.commit()
        print("Schema 'ops' created successfully.")
    except psycopg2.Error as e:
        print(f"Error creating schema: {e}")
        conn.rollback()

    try:
        with open("/workspace/kafka/data/ops/static_customer_informations.csv", "r") as f:
            cursor.copy_expert(
                """
                COPY ops.customers (customer_id, phone_number, email)
                FROM STDIN WITH CSV HEADER
                """,
                f
            )

        with open("/workspace/kafka/data/ops/static_products.csv", "r") as f:
            cursor.copy_expert(
                """
                COPY ops.products (product_id, category_id, category_code, brand, price)
                FROM STDIN WITH CSV HEADER
                """,
                f
            )
        print("Initial data imported successfully into 'ops' schema.")

        conn.commit()
    except psycopg2.Error as e:
        print(f"Error importing data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def import_initial_data_to_pos() -> None:
    """
    Imports initial data into the 'pos' table in the PostgreSQL database.
    """
    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("DROP SCHEMA IF EXISTS pos CASCADE;")

        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS pos;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pos.customers (
                customer_id BIGINT PRIMARY KEY,
                phone_number VARCHAR(20) NOT NULL,
                email VARCHAR(255),
                citizen_identification_number VARCHAR(20)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pos.order_item (
                product_id BIGINT,
                order_id VARCHAR(20),
                quantity INT,
                unit_price DECIMAL(10, 2)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pos.orders (
                order_id VARCHAR(20),
                customer_id BIGINT,
                total_amount DECIMAL(10, 2),
                update_at VARCHAR(20) NOT NULL
            );
        """)

        conn.commit()
        print("Schema 'pos' created successfully.")
    except psycopg2.Error as e:
        print(f"Error creating schema: {e}")
        conn.rollback()

    try:
        with open("/workspace/kafka/data/pos/static_customer_informations.csv", "r") as f:
            cursor.copy_expert(
                """
                COPY pos.customers (customer_id, phone_number, email, citizen_identification_number)
                FROM STDIN WITH CSV HEADER
                """,
                f
            )

        print("Initial data imported successfully into 'pos' schema.")

        conn.commit()
    except psycopg2.Error as e:
        print(f"Error importing data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def process_pos_records(record: Dict[str, Any]) -> None:
    """
    Processes POS records and inserts them into the 'pos' schema in the PostgreSQL database.

    Args:
        records (Dict[str, Any]): A dictionary containing POS records.
    """
    conn = get_postgres_connection()
    cursor = conn.cursor()

    if record.get("event_type") == "join":
        customer_id = record.get("user_id")
        phone_number = record.get("phone_number")
        email = record.get("email")
        citizen_identification_number = record.get("citizen_identification_number")

        try:
            cursor.execute("""
                INSERT INTO pos.customers (customer_id, phone_number, email, citizen_identification_number)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (customer_id) DO NOTHING;
            """, (customer_id, phone_number, email, citizen_identification_number))
            conn.commit()
        except psycopg2.Error as e:
            print(f"Error inserting join event: {e}")
            conn.rollback()
    elif record.get("event_type") == "purchase" & record.get("items") != None:
        customer_id = record.get("user_id") or record.get("customer_id")
        order_id = record.get("order_id")
        total_amount = record.get("total_amount")
        update_at = record.get("event_time")

        print(record)
        order_item_records = [
            (
                item.get("product_id"),
                order_id,
                item.get("quantity"),
                item.get("unit_price")
            ) for item in record.get("items") 
        ]

        try:
            cursor.execute("""
                INSERT INTO pos.orders 
                (
                    order_id, 
                    customer_id,
                    total_amount, 
                    update_at
                )
                VALUES (%s, %s, %s, %s);
            """, (order_id, customer_id, total_amount, update_at))

            cursor.executemany(
                """
                INSERT INTO pos.order_item
                (
                    product_id,
                    order_id,
                    quantity,
                    unit_price
                )
                VALUES (%s, %s, %s, %s)
                """,
                order_item_records
            )
            conn.commit()
        except psycopg2.Error as e:
            print(f"Error inserting purchase event: {e}")
            conn.rollback()
    cursor.close()
    conn.close()

def process_ops_records(record: Dict[str, Any]) -> None:
    """
    Processes OPS records and inserts them into the 'ops' schema in the PostgreSQL database.

    Args:
        records (Dict[str, Any]): A dictionary containing OPS records.
    """
    conn = get_postgres_connection()
    cursor = conn.cursor()

    if record.get("event_type") == "join":
        customer_id = record.get("user_id")
        phone_number = str(record.get("phone_number"))
        email = record.get("email")
        try:
            cursor.execute("""
                INSERT INTO ops.customers 
                (
                    customer_id, 
                    phone_number, 
                    email
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (customer_id) DO NOTHING;
            """, (customer_id, phone_number, email))
            conn.commit()
        
        except psycopg2.Error as e:
            print(f"Error inserting purchase event: {e}")
            conn.rollback()
    elif record.get("event_type") == "purchase":
        customer_id = record.get("user_id")
        product_id = record.get("product_id")
        time = record.get("event_time")
        try:
            cursor.execute("""
                INSERT INTO ops.orders 
                (
                    customer_id, 
                    product_id, 
                    time
                )
                VALUES (%s, %s, %s);
            """, (customer_id, product_id, time))
            conn.commit()
        
        except psycopg2.Error as e:
            print(f"Error inserting purchase event: {e}")
            conn.rollback()
    cursor.close()
    conn.close()