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

    bronn_section = soup.find('li', id='brønnhistorie')
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

    # Collect all elements within content_div
    elements = content_div.find_all(['div', 'span'], recursive=True)

    for element in elements:
        if element.name == 'div':
            continue  # Skip divs if they don't contain useful content
        elif element.name == 'span':
            text = element.get_text(strip=True)
            style = element.get('style', '')
            if 'font-weight:700' in style:
                # Header detected
                if current_section and current_content:
                    sections.append((current_section, current_content.strip()))
                    current_content = ''
                current_section = text
            else:
                current_content += ' ' + text
        else:
            continue  # Skip other elements

    # Save the last section
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
