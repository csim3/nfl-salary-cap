import os
from sqlalchemy import create_engine
import pymysql

# Constants
DATABASE_NAME = 'nfl'
TABLE_NAME = 'cap_tracker'

# Environment variables
MYSQL_HOST = os.environ.get('MYSQL_HOST')
MYSQL_USER = os.environ.get('MYSQL_USER')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD')


def get_database_connection():
    """Establishes a connection to the MySQL database.

    Returns:
        Connection: A pymysql connection object configured to use a dictionary cursor.

    Raises:
        pymysql.MySQLError: If the connection to the MySQL database fails, an error is 
            raised with the reason for the failure.
    """
    return pymysql.connect(
        host = MYSQL_HOST,
        user = MYSQL_USER,
        password = MYSQL_PASSWORD,
        database = DATABASE_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

def delete_from_mysql(table_name, team):
    """Deletes records from a MySQL table for a specified team using parameterized queries.

    Args:
        table_name (str): The name of the MySQL table from which to delete records.
        team (str): The name of the NFL team for which records should be deleted.

    Returns:
        None

    Raises:
        pymysql.MySQLError: If there's an issue with the database operation, 
            it raises an error with details.
    """
    with get_database_connection() as connection:
        with connection.cursor() as cursor:
            query = f"DELETE FROM {table_name} WHERE team=%s;"
            cursor.execute(query, (team,))
        connection.commit()

def insert_into_mysql(df):
    """Inserts data from a pandas DataFrame into a MySQL database table.

    Args:
        df (pandas.DataFrame): The DataFrame containing the data to be inserted into the 
            MySQL table.

    Returns:
        None

    Raises:
        SQLAlchemyError: An error can occur during the creation of the engine or the 
            insertion process.
    """
    connection_str = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{DATABASE_NAME}"
    engine = create_engine(connection_str)
    with engine.connect() as connection:
        df.to_sql(TABLE_NAME, con=connection, if_exists='append', index=False)
