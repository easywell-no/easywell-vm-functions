import requests
from bs4 import BeautifulSoup
import logging
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
                'Accept-Language': 'no'  # Request Norwegian language content
            }
            response = requests.get(factpage_url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = response.apparent_encoding  # Ensure correct encoding
            html_content = response.text
            break  # Exit loop if successful
        except (HTTPError, ConnectionError, Timeout) as e:
            logging.error(f"Network error fetching the factpage for {wlbwellborename}: {e}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying... ({attempt + 1}/{max_retries})")
                time.sleep(2)  # Wait before retrying
            else:
                logging.error(f"Failed to fetch factpage after {max_retries} attempts for {wlbwellborename}")
                return
        except RequestException as e:
            logging.error(f"Error fetching the factpage for {wlbwellborename}: {e}")
            return

    if not html_content:
        logging.error(f"No HTML content retrieved for {wlbwellborename}")
        return

    soup = BeautifulSoup(html_content, 'html.parser')

    # Debug: Log the entire HTML to inspect structure
    logging.debug(f"HTML content for {wlbwellborename}: {html_content}")

    # Broaden search for the general info section
    general_info_table = None
    for table in soup.find_all('table'):
        if table.find(text=lambda t: 'Brønnbane navn' in t):
            general_info_table = table
            break

    if not general_info_table:
        logging.warning(f"'Brønnbane navn' not found in any table for {wlbwellborename}")
        return
    else:
        logging.info(f"Found the general info table for {wlbwellborename}")

    # Initialize a dictionary to hold the scraped data
    data = {
        'wlbwellborename': wlbwellborename.strip()
    }

    # Define a mapping from the HTML labels to database column names
    label_to_column = {
        'Brønnbane navn': 'bronnbane_navn',
        'Type': 'type',
        'Formål': 'formål',
        'Status': 'status',
        'Felt': 'felt',
        # Add more mappings as needed...
    }

    # Function to parse date strings into YYYY-MM-DD format
    def parse_date(date_str):
        try:
            return datetime.strptime(date_str, '%d.%m.%Y').strftime('%Y-%m-%d')
        except ValueError:
            logging.warning(f"Invalid date format '{date_str}' for {wlbwellborename}")
            return None

    # Iterate over the table rows to extract label-value pairs
    for row in general_info_table.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) != 2:
            logging.debug(f"Skipping row with {len(cells)} cells.")
            continue  # Skip rows that don't have exactly two cells

        label = cells[0].get_text(strip=True).rstrip(':')
        value = cells[1].get_text(strip=True)

        logging.debug(f"Extracted label: '{label}', value: '{value}'")

        if label in label_to_column:
            column = label_to_column[label]
            if column in ['borestart', 'boreslutt', 'frigitt_dato']:
                data[column] = parse_date(value)
            else:
                data[column] = value

    # Log the data to be upserted
    logging.debug(f"Data to upsert for {wlbwellborename}: {data}")

    # Upsert the data into the 'general_info' table
    try:
        response = supabase.table('general_info') \
            .upsert(data, on_conflict='wlbwellborename') \
            .execute()
        if response.status_code in [200, 201]:
            logging.info(f"Upserted general_info data for {wlbwellborename}")
        else:
            logging.error(f"Failed to upsert general_info data for {wlbwellborename}: {response.status_code}")
    except Exception as e:
        logging.error(f"Exception during database operation for {wlbwellborename}: {e}")
