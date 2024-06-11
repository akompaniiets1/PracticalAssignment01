import mysql.connector
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from datetime import datetime
import os

# Load environment variables
load_dotenv()

# Connection settings
HOST = os.getenv('localhost')
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


def read_repeatable():
    """
    Shows how REPEATABLE READ isolation level works.
    Shows phantom reads
    :return: void
    """
    global cursor1, cursor2
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()
        # Transaction 1: Repeatable read
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction(isolation_level='REPEATABLE READ')
        cursor1.execute("SELECT * FROM accounts")
        data = cursor1.fetchall()
        print(f'Data in the table: {data}')

        # Transaction 2:
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction(isolation_level='REPEATABLE READ')
        cursor2.execute("INSERT INTO accounts (name, balance) VALUES ('George', 3000)")
        connection2.commit()
        print(f"Transaction 2 commit(): {datetime.now()}")

        # Transaction 1 re-reads the balance
        cursor1.execute("SELECT * FROM accounts")
        changed_data = cursor1.fetchall()
        print(f"Phantom read: appearing of a new row = {changed_data}")

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
    read_repeatable()