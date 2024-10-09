import requests
from bs4 import BeautifulSoup
import logging
from supabase import Client
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import time

def scrape_wellbore_history(supabase: Client, wlbwellborename: str, factpage_url: str):
    max_retries = 1  # Set to a positive integer
    html_content = None  # Initialize html_content

    # Delete existing data for the wellbore
    try:
        supabase.table('wellbore_history')\
            .delete()\
            .eq('wlbwellborename', wlbwellborename)\
            .execute()
        logging.info(f"Deleted existing wellbore history data for {wlbwellborename}")
    except Exception as e:
        logging.error(f"Failed to delete existing wellbore history data for {wlbwellborename}: {e}")

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

    # For debugging: log all h2 texts found within li elements
    logging.debug(f"Searching for 'Brønnhistorie' or 'Well history' sections in {wlbwellborename}")
    bronn_section = None
    for li in soup.find_all('li'):
        h2 = li.find('h2')
        if h2:
            h2_text = h2.get_text(strip=True)
            logging.debug(f"Found h2 text: '{h2_text}'")
            if h2_text in ['Brønnhistorie', 'Well history', 'Wellbore history']:
                bronn_section = li
                break

    if not bronn_section:
        logging.warning(f"'Brønnhistorie' or 'Well history' section not found for {wlbwellborename}")
        return
    else:
        logging.info(f"Found '{h2_text}' section for {wlbwellborename}")

    content_div = bronn_section.find('div', class_='uk-accordion-content')
    if not content_div:
        logging.warning(f"No content found in '{h2_text}' for {wlbwellborename}")
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

    # Log the sections found
    logging.debug(f"Sections found for {wlbwellborename}: {[(s[0], len(s[1])) for s in sections]}")

    # Insert or update the database
    for section, content in sections:
        try:
            # Query existing content
            existing_record = supabase.table('wellbore_history') \
                .select('content') \
                .eq('wlbwellborename', wlbwellborename) \
                .eq('section', section) \
                .execute()
            existing_content = existing_record.data[0]['content'] if existing_record.data else None

            if existing_content == content:
                logging.info(f"No changes detected for {wlbwellborename} section '{section}', skipping update.")
                continue  # Skip to next section

            data = {
                'wlbwellborename': wlbwellborename,
                'section': section,
                'content': content
            }
            # Upsert operation: Update if exists, else insert
            response = supabase.table('wellbore_history') \
                .upsert(data, on_conflict='wlbwellborename,section') \
                .execute()
            logging.info(f"Upserted {section} for {wlbwellborename}")
        except Exception as e:
            logging.error(f"Exception during database operation for {wlbwellborename}: {e}")
