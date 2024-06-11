import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from datetime import datetime
import os

# Load environment variables
load_dotenv()

# Connection settings
HOST = 'localhost'  # Make sure you have a HOST environment variable set
USER = 'root'
PASSWORD = 'Data@base#2024'
DATABASE = 'practice'


def create_connection():
    try:
        connection = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error: {e}")
    return None


def initialize_database():
    """
    Initializes the database with the initial balance for Alice.
    """
    connection = create_connection()
    if connection is None:
        print("Failed to create connection for initialization")
        return


def read_committed():
    """
    Shows how READ COMMITTED isolation level works.
    Shows non-repeatable read.
    """
    connection1 = create_connection()
    connection2 = create_connection()


    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Transaction 1: Read Committed
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction(isolation_level='READ COMMITTED')
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        Alice_balance_before = cursor1.fetchone()[0]
        print(f"NON-REPEATABLE READ (READ COMMITTED): Alice's balance before = {Alice_balance_before}")

        # Fetch all remaining rows to ensure no unread results

        # Transaction 2: Read Committed
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction(isolation_level='READ COMMITTED')
        cursor2.execute("UPDATE accounts SET balance = 9999 WHERE name = 'Alice'")
        print(f"Transaction 2 commit(): {datetime.now()}")
        connection2.commit()

        # Transaction 1 re-reads the balance
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        Alice_balance_after = cursor1.fetchone()[0]
        print(f"NON-REPEATABLE READ (READ COMMITTED): Alice's balance after = {Alice_balance_after}")

        print(f"Transaction 1 commit(): {datetime.now()}")
        connection1.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()


if __name__ == "__main__":
    # Initialize the database
    initialize_database()

    # Demonstrate read committed
    read_committed()
