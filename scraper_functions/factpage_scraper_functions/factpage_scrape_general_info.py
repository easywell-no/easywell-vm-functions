import requests
from bs4 import BeautifulSoup
from supabase import Client
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
from datetime import datetime
import time

def scrape_general_info(supabase: Client, wlbwellborename: str, factpage_url: str):
    max_retries = 3
    html_content = None

    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0',
                'Accept-Language': 'no'
            }
            response = requests.get(factpage_url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            html_content = response.text
            break  # Exit loop if successful
        except (HTTPError, ConnectionError, Timeout):
            if attempt < max_retries - 1:
                time.sleep(2)  # Wait before retrying
            else:
                return  # Exit if retries exhausted
        except RequestException:
            return

    if not html_content:
        return

    soup = BeautifulSoup(html_content, 'html.parser')

    # Search for the general info section
    general_info_table = None
    for table in soup.find_all('table'):
        if table.find(text=lambda t: 'Brønnbane navn' in t):
            general_info_table = table
            break

    if not general_info_table:
        return

    # Initialize a dictionary to hold the scraped data
    data = {'wlbwellborename': wlbwellborename.strip()}

    # Define a mapping from the HTML labels to database column names
    label_to_column = {
        'Brønnbane navn': 'bronnbane_navn',
        'Type': 'type',
        'Formål': 'formål',
        'Status': 'status',
        'Felt': 'felt',
    }

    # Function to parse date strings into YYYY-MM-DD format
    def parse_date(date_str):
        try:
            return datetime.strptime(date_str, '%d.%m.%Y').strftime('%Y-%m-%d')
        except ValueError:
            return None

    # Iterate over the table rows to extract label-value pairs
    for row in general_info_table.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) != 2:
            continue  # Skip rows that don't have exactly two cells

        label = cells[0].get_text(strip=True).rstrip(':')
        value = cells[1].get_text(strip=True)

        if label in label_to_column:
            column = label_to_column[label]
            if column in ['borestart', 'boreslutt', 'frigitt_dato']:
                data[column] = parse_date(value)
            else:
                data[column] = value

    # Upsert the data into the 'general_info' table
    try:
        response = supabase.table('general_info') \
            .upsert(data, on_conflict='wlbwellborename') \
            .execute()
        return response.status_code in [200, 201]
    except Exception:
        return False
