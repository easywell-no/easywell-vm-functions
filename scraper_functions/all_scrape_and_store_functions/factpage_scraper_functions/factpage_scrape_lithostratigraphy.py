import requests
from bs4 import BeautifulSoup
import logging
from supabase import Client
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import time

def scrape_lithostratigraphy(supabase: Client, wlbwellborename: str, factpage_url: str):
    max_retries = 1
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

    # Find the lithostratigraphy section
    litho_section = soup.find('li', id='litostratigrafi')
    if not litho_section:
        logging.warning(f"'Litostratigrafi' section not found for {wlbwellborename}")
        return
    else:
        logging.info(f"Found 'Litostratigrafi' section for {wlbwellborename}")

    # Find the table within the section
    table = litho_section.find('table', class_='a1820 uk-table-striped')
    if not table:
        logging.warning(f"No lithostratigraphy table found for {wlbwellborename}")
        return

    tbody = table.find('tbody')
    if not tbody:
        logging.warning(f"No table body found in lithostratigraphy table for {wlbwellborename}")
        return

    rows = tbody.find_all('tr')
    if not rows:
        logging.warning(f"No data rows found in lithostratigraphy table for {wlbwellborename}")
        return

    for row in rows:
        cells = row.find_all('td')
        if len(cells) != 2:
            logging.warning(f"Unexpected number of cells in row for {wlbwellborename}")
            continue
        depth_cell = cells[0]
        unit_cell = cells[1]
        # Extract text
        depth_text = depth_cell.get_text(strip=True)
        unit_text = unit_cell.get_text(strip=True)
        # Process the data
        try:
            depth = float(depth_text.replace(',', '.'))  # Handle commas
        except ValueError:
            logging.warning(f"Invalid depth value '{depth_text}' for {wlbwellborename}")
            continue

        data = {
            'wlbwellborename': wlbwellborename,
            'top_depth_m_md_rkb': depth,
            'lithostratigraphic_unit': unit_text
        }

        # Upsert the data
        try:
            response = supabase.table('lithostratigraphy') \
                .upsert(data, on_conflict='wlbwellborename,top_depth_m_md_rkb,lithostratigraphic_unit') \
                .execute()
            logging.info(f"Upserted lithostratigraphy data for {wlbwellborename} at depth {depth}")
        except Exception as e:
            logging.error(f"Exception during database operation for {wlbwellborename}: {e}")
