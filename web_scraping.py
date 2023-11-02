import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

from sql_operations import insert_into_mysql, delete_from_mysql
from gsheets_operations import truncate_google_sheet, insert_google_sheet

# Constants
BASE_URL = "https://www.spotrac.com/nfl/"
TABLE_NAME = 'cap_tracker'
GOOGLE_SHEETS_FILE = 'nfl_salary_cap_data'
GOOGLE_SHEETS_SHEET = 'players_cap_hits'

def generate_soup(url):
    """Send a GET request to the specified URL and return the parsed HTML content.

    Args:
        url (str): The URL of the webpage to scrape.

    Returns:
        BeautifulSoup: Parsed HTML content of the page.

    Raises:
         HTTPError: An error occurs from the HTTP request.
    """
    response = requests.get(url)
    response.raise_for_status()  # Raise HTTPError for bad responses
    return BeautifulSoup(response.text, 'html.parser')

def create_df_from_soup(roster_status_table, roster_status_player, team, soup):
    """Create a DataFrame from parsed HTML table content.

    Args:
        roster_status_table (str): Roster type of the cap table to be processed that 
            identifies the table in the HTML content (e.g., 'active', '2023 Injured 
            Reserve Cap', '2023 Dead Cap').
        roster_status_player (str): Roster status of player to which the table data 
            pertains (e.g., 'active', 'ir', 'dead cap').
        team (str): The name of the NFL team to which the data pertains.
        soup (BeautifulSoup): The BeautifulSoup object containing the parsed HTML content 
            where the table is located.

    Returns:
        DataFrame: A pandas DataFrame containing the information extracted from the HTML 
            table, with columns for 'player_name', 'position', 'cap_hit', 'roster_status', 
            and 'team'.

    Raises:
        AssertionError: If the total cap hit calculated from the DataFrame does not match 
            the expected total extracted from the HTML table.
    """
    df = pd.DataFrame(columns=['player_name', 'position', 'cap_hit', 'roster_status', 'team'])

    if roster_status_table=="active":
        table_body = soup.find('table', {'class': 'datatable rtable'})
    else:
        header = soup.find('h2', string=roster_status_table)
        if not header:
            return df
        else:
            table_body = header.find_next('table', {'class': 'datatable rtable'})
    
    data = extract_rows_from_body(table_body, roster_status_player, team)
    df = pd.DataFrame(data, columns=df.columns)
    verify_table_total(df, table_body)
    return df

def extract_table_total_cap_hit(table_body):
    """Extracts the total cap hit for a roster type from the html table's footer.

    Args:
        table_body (bs4.element.Tag): The BeautifulSoup object representing the body of the 
            table from which to extract the total cap hit.

    Returns:
        int: The total cap hit value extracted from the table, or 0 if extraction fails.

    Raises:
        ValueError: If the string extracted from the table can't be converted to an integer.
    """
    tfoot = table_body.find('tfoot')
    cap_hit_td = tfoot.find('td', {'class': 'right result xs-visible'})
    cap_hit_span = cap_hit_td.find('span', {'title': 'Cap Hit'})
    total_cap_hit = cap_hit_span.text.strip()[1:].replace(',', '')
    try:
        total_cap_hit_int = int(total_cap_hit)
    except ValueError:
        total_cap_hit_int = 0
    return total_cap_hit_int

def verify_table_total(df, table_body):
    """Verifies that the sum of 'cap_hit' in the DataFrame matches the extracted total 
        from the html table body for a roster type.

    Args:
        df (pandas.DataFrame): The DataFrame containing player cap hit data to verify.
        table_body (bs4.element.Tag): The BeautifulSoup object representing the body of 
            the table from which to extract and compare the total cap hit.

    Raises:
        AssertionError: If the sum of the 'cap_hit' in the DataFrame does not match the 
            extracted total cap hit from the table body.
    """
    actual_sum = df['cap_hit'].sum()
    expected_sum = extract_table_total_cap_hit(table_body)
    error_message = (
        f"Expected cap_hit sum to be {expected_sum}, "
        f"but got {actual_sum}"
    )
    assert actual_sum == expected_sum, error_message

def extract_row_data(row, roster_status_player, team):
    """Extracts the data from a single row of the NFL cap hit table for a single player.

    Args:
        row (bs4.element.Tag): A BeautifulSoup object representing a single table row.
        roster_status_player (str): The roster type of a player (e.g., 'active', 
            'ir', 'dead cap').
        team (str): The name of the NFL team to which the player belongs.

    Returns:
        list: A list containing the extracted data for a single player, including the 
            player's name, position, cap hit, roster status, and team name. If data 
            for any column cannot be extracted, the corresponding element in the list 
            will be None.

    Example:
        ['John Doe', 'QB', 5000000, 'active', 'new-england-patriots']  
            # possible return value if all data is present
        [None, 'QB', None, 'active', 'new-england-patriots']  
            # possible return value if some data can't be extracted
    """
    try:
        player = row.find('td', {'class': 'player'}).find('a').text
    except AttributeError:
        player = None
        print(f"Row can't be extracted for {roster_status_player} table on {team}")
    try:
        pos = row.find('td', {'class': 'center'}).text.strip()
    except AttributeError:
        pos = None
        print(f"Row can't be extracted for position for {player} on {team}")
    try:
        cap_hit_td = row.find('td', {'class': re.compile('^right result')})
        cap_hit_span = cap_hit_td.find('span', {'title': re.compile('^Cap Hit')})
        cap_hit = cap_hit_span.text.strip()[1:].replace(',', '')
        cap_hit = int(cap_hit)
    except (AttributeError, ValueError) :
        cap_hit = None
        print(f"Row can't be extracted for cap hit for {player} on {team}")
    return [player, pos, cap_hit, roster_status_player, team]

def extract_rows_from_body(table_body, roster_status_player, team):
    """Extracts data from all the rows within a table body for a specified team and 
        roster status.

    Args:
        table_body (bs4.element.Tag): A BeautifulSoup object representing the body 
            of the table from which to extract the data.
        roster_status_player (str): The roster status of the player (e.g., 'active', 
            'ir', 'dead cap').
        team (str): The name of the NFL team for which the data is being extracted.

    Returns:
        list of list: A list of lists, where each inner list contains data about a 
            single player, including the player's name, position, cap hit, roster 
            status, and team name. 
            For example: [[player_name, position, cap_hit, roster_status, team], [...], ...]
    """
    rows = table_body.find('tbody').findAll('tr')
    data = []
    for row in rows:
        row_data = extract_row_data(row, roster_status_player, team)
        data.append(row_data)
    return data

def extract_nfl_teams():
    """Extracts the names of the 32 NFL teams from a specific BASE_URL and formats 
        the team names.

    Returns:
        list of str: A list of formatted NFL team names with spaces replaced by hyphens. 
            For example: ['new-england-patriots', 'buffalo-bills', ...]

    Raises:
        AssertionError: If the number of teams extracted does not equal 32, an assertion 
            error is raised with a message indicating the number of teams found.
    """
    soup = generate_soup(BASE_URL)
    subnav_posts = soup.find('li', 
                             class_='cat-nfl active').find('div', class_='subnav-posts')
    teams = [a.text.lower() for a in subnav_posts.find_all('a')]
    assert len(teams) == 32, f"Extracted {len(teams)} instead of 32 NFL teams"
    teams_reformatted = [team.replace(' ','-') for team in teams]
    return teams_reformatted

def extract_team_total_cap_hit(soup):
    """Extracts the total cap hit for a team from the given soup object.

    Args:
        soup (bs4.BeautifulSoup): A BeautifulSoup object containing the parsed HTML 
            from where the cap total needs to be extracted.

    Returns:
        int: The total cap hit value for the team as an integer.

    Raises:
        AttributeError: If the expected table or row elements are not found in the 
            HTML structure, an AttributeError will be raised by BeautifulSoup.
    """
    header = soup.find('h2', string="2023 Cap Totals")
    table_body = header.find_next('table', {'class': 'datatable rtable captotal'})
    total_row = table_body.find('td', string='Total').find_parent('tr')
    total_value_td = total_row.find_all('td')[-1]
    total_cap_hit = total_value_td.text.strip()[1:].replace(',', '')
    return int(total_cap_hit)

def verify_team_total(df, soup, team):
    """Verifies that the sum of 'cap_hit' values in the dataframe matches the expected 
        team total cap hit.

    Args:
        df (pandas.DataFrame): A dataframe containing player contract information, 
            where the 'cap_hit' column holds the cap hit values to sum up.
        soup (bs4.BeautifulSoup): A BeautifulSoup object that contains the HTML data 
            from which the expected team total cap hit is extracted.
        team (str): The name of the NFL team for which the verification is being 
            performed. Used in the error message to indicate which team's totals are 
            being verified.

    Raises:
        AssertionError: If the actual sum of 'cap_hit' from the dataframe does not 
            equal the expected team total cap hit extracted from 'soup'.
        """
    actual_sum = df['cap_hit'].sum()
    expected_sum = extract_team_total_cap_hit(soup)
    error_message = (
        f"Expected cap_hit sum for {team} to be {expected_sum}, "
        f"but got {actual_sum}"
    )
    assert actual_sum == expected_sum, error_message

def fetch_data_for_team(team):
    """Fetches and aggregates player salary cap data for a given NFL team from a website.

    Args:
        team (str): The team identifier used in the URL and to tag the data. It should 
            correspond to the particular segment in the URL that refers to the team on 
            the website.

    Returns:
        pandas.DataFrame: A DataFrame containing the aggregated salary cap data for 
            the specified team across all relevant roster categories.

    Notes:
        - The function relies on several helper functions to operate correctly:
            - `generate_soup(url)`: to create a BeautifulSoup object from the HTML at 
                the team URL.
            - `create_df_from_soup(roster_status_table, roster_status_player, team, soup)`: 
                to create DataFrames for each roster category from the BeautifulSoup object.
            - `verify_team_total(df, soup, team)`: to verify that the total 'cap_hit' 
                in the DataFrame matches the expected total on the website.
        - An empty DataFrame is not appended to the list of DataFrames to be concatenated.
        - The function prints a message confirming that the total cap hit has been 
            verified for the team.
    """
    team_url = f"{BASE_URL}{team}/cap/"
    tables = [
        ('active', 'active'),
        ('2023 Reserve/Suspended Cap', 'reserve/suspended'),
        ('2023 Exempt/Commissioner’s Permission List', 'exempt'),
        ('2023 Injured Reserve Cap', 'ir'),
        ('2023 Reserve/PUP', 'pup'),
        ('2023 Non-Football Injury Cap', 'non-football injury'),
        ('2023 Practice Squad', 'practice squad'),
        ('2023 Dead Cap', 'dead cap')
    ]
    soup = generate_soup(team_url)
    dfs = []
    for roster_status_table, roster_status_player in tables:
        df = create_df_from_soup(roster_status_table, roster_status_player, team, soup)
        if not df.empty:
            dfs.append(df)
    team_df = pd.concat(dfs, ignore_index=True)
    verify_team_total(team_df, soup, team)
    print(f"{team}: Total cap checks out")
    return team_df

def main():
    """Main execution function for fetching and updating NFL teams' salary cap data.
    """
    teams = extract_nfl_teams()
    for team in teams:
        delete_from_mysql(TABLE_NAME, team)
        df = fetch_data_for_team(team)
        insert_into_mysql(df)
        
    truncate_google_sheet(GOOGLE_SHEETS_FILE, GOOGLE_SHEETS_SHEET)
    insert_google_sheet(GOOGLE_SHEETS_FILE, GOOGLE_SHEETS_SHEET, TABLE_NAME)
    
if __name__ == '__main__':
    main()