import os
import pandas as pd
import pygsheets
from sql_operations import get_database_connection

# Constants
GOOGLE_SHEETS_FILE = 'nfl_salary_cap_data'
GOOGLE_SHEETS_SHEET = 'players_cap_hits'
TABLE_NAME = 'cap_tracker'

# Environment variables
PYGSHEETS_FILE_CREDENTIALS = os.environ.get('PYGSHEETS_FILE')

def truncate_google_sheet(gsheets_file, gsheets_sheet):
    """Delete all rows (except header row) of a Google Sheet.
    """
    try:
        client = pygsheets.authorize(service_account_file=PYGSHEETS_FILE_CREDENTIALS)
        sht = client.open(gsheets_file)
        wks = sht.worksheet('title',gsheets_sheet)
        wks.clear('A2')
        print(f"Data truncated from {gsheets_sheet}")
    except Exception as e:
        print(f"Data did not truncate from {gsheets_sheet}. Error: {e}")
    
def insert_google_sheet(gsheets_file, gsheets_sheet, mysql_table):
    """Insert MySQL db table into Google Sheet
    """
    try:
        client = pygsheets.authorize(service_account_file=PYGSHEETS_FILE_CREDENTIALS)
        sht = client.open(gsheets_file)
        wks = sht.worksheet('title', gsheets_sheet)

        with get_database_connection() as connection:
            with connection.cursor() as cursor:
                query = f"SELECT * FROM {mysql_table};"
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                data_df = pd.DataFrame(rows, columns=columns)
        wks.set_dataframe(data_df, (2,1), copy_head=False, extend=True, nan='')
        print(f"Data uploaded to {gsheets_sheet}")
    except Exception as e:
        print(f"Data did not uload to {gsheets_sheet}. Error: {e}")
