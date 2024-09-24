# factpage_scrape_wellbore_history.py

import requests
from bs4 import BeautifulSoup
import logging
from supabase import Client
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import time

def scrape_wellbore_history(supabase: Client, wlbwellborename: str, factpage_url: str):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0'
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
                return
        except RequestException as e:
            logging.error(f"Error fetching the factpage for {wlbwellborename}: {e}")
            return

    soup = BeautifulSoup(html_content, 'html.parser')

    # Find the 'li' element containing the 'Brønnhistorie' section by searching for the 'h2' tag
    bronn_section = None
    for li in soup.find_all('li'):
        h2 = li.find('h2')
        if h2 and h2.get_text(strip=True) == 'Brønnhistorie':
            bronn_section = li
            break

    if not bronn_section:
        logging.warning(f"'Brønnhistorie' section not found for {wlbwellborename}")
        return

    content_div = bronn_section.find('div', class_='uk-accordion-content')
    if not content_div:
        logging.warning(f"No content found in 'Brønnhistorie' for {wlbwellborename}")
        return

    # Initialize variables
    sections = []
    current_section = None
    current_content = ''

    # Collect all relevant child elements
    for child in content_div.find_all(['span', 'div'], recursive=True):
        if child.name == 'span':
            text = child.get_text(strip=True)
            style = child.get('style', '')
            if 'font-weight:700' in style:
                # Header detected
                if current_section and current_content:
                    sections.append((current_section, current_content.strip()))
                    current_content = ''
                current_section = text
            else:
                current_content += ' ' + text
        elif child.name == 'div':
            # If we encounter a div that might contain relevant text
            for sub_child in child.find_all('span', recursive=False):
                text = sub_child.get_text(strip=True)
                current_content += ' ' + text

    # Save the last section if it exists
    if current_section and current_content:
        sections.append((current_section, current_content.strip()))

    # Insert or update the database
    for section, content in sections:
        try:
            data = {
                'wlbwellborename': wlbwellborename,
                'section': section,
                'content': content
            }
            # Upsert operation: Update if exists, else insert
            response = supabase.table('wellbore_history').upsert(data, on_conflict=['wlbwellborename', 'section']).execute()
            if response.error:
                logging.error(f"Error upserting data into wellbore_history for {wlbwellborename}: {response.error}")
            else:
                logging.info(f"Upserted {section} for {wlbwellborename}")
        except Exception as e:
            logging.error(f"Exception during database operation for {wlbwellborename}: {e}")
