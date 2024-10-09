import requests
from bs4 import BeautifulSoup
import logging
from supabase import Client
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import time

def scrape_casing_and_tests(supabase: Client, wlbwellborename: str, factpage_url: str):
    max_retries = 3
    html_content = None

    try:
        supabase.table('casing_and_tests')\
            .delete()\
            .eq('wlbwellborename', wlbwellborename)\
            .execute()
        logging.info(f"Deleted existing casing and tests data for {wlbwellborename}")
    except Exception as e:
        logging.error(f"Failed to delete existing casing and tests data for {wlbwellborename}: {e}")
    

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

    # Find the casing and tests section
    casing_section = soup.find('li', id='foringsrør-og-formasjonsstyrketester')
    if not casing_section:
        logging.warning(f"'Foringsrør og formasjonsstyrketester' section not found for {wlbwellborename}")
        return
    else:
        logging.info(f"Found 'Foringsrør og formasjonsstyrketester' section for {wlbwellborename}")

    # Find the table within the section
    table = casing_section.find('table', class_='a2342 uk-table-striped')
    if not table:
        logging.warning(f"No casing and tests table found for {wlbwellborename}")
        return

    tbody = table.find('tbody')
    if not tbody:
        logging.warning(f"No table body found in casing and tests table for {wlbwellborename}")
        return

    rows = tbody.find_all('tr')
    if not rows:
        logging.warning(f"No data rows found in casing and tests table for {wlbwellborename}")
        return

    for row in rows:
        cells = row.find_all('td')
        if len(cells) != 7:
            logging.warning(f"Unexpected number of cells ({len(cells)}) in row for {wlbwellborename}")
            continue
        # Extract data from cells
        casing_type_cell = cells[0]
        casing_diameter_cell = cells[1]
        casing_depth_cell = cells[2]
        borehole_diameter_cell = cells[3]
        borehole_depth_cell = cells[4]
        mud_weight_cell = cells[5]
        test_type_cell = cells[6]

        # Get text and strip whitespace
        casing_type = casing_type_cell.get_text(strip=True)
        casing_diameter_text = casing_diameter_cell.get_text(strip=True)
        casing_depth_text = casing_depth_cell.get_text(strip=True)
        borehole_diameter_text = borehole_diameter_cell.get_text(strip=True)
        borehole_depth_text = borehole_depth_cell.get_text(strip=True)
        mud_weight_text = mud_weight_cell.get_text(strip=True)
        test_type = test_type_cell.get_text(strip=True)

        # Updated parse_float function
        def parse_float(text):
            if not text:
                return None
            try:
                text = text.strip()
                # Handle fractions (e.g., '13 3/8' -> 13.375)
                if ' ' in text:
                    # Split into whole number and fraction
                    whole_part, frac_part = text.split(' ', 1)
                    whole_part = float(whole_part)
                    frac_part = frac_part.strip()
                    if '/' in frac_part:
                        numerator, denominator = frac_part.split('/', 1)
                        numerator = float(numerator)
                        denominator = float(denominator)
                        frac_value = numerator / denominator
                        value = whole_part + frac_value
                    else:
                        # Not a fraction, try to parse as float
                        frac_value = float(frac_part)
                        value = whole_part + frac_value
                elif '/' in text:
                    # No whole number, only fraction (e.g., '3/8')
                    numerator, denominator = text.split('/', 1)
                    numerator = float(numerator)
                    denominator = float(denominator)
                    value = numerator / denominator
                else:
                    # Regular float
                    value = float(text.replace(',', '.'))
                return value
            except ValueError:
                logging.warning(f"Invalid float value '{text}' for {wlbwellborename}")
                return None

        casing_diameter_inches = parse_float(casing_diameter_text)
        casing_depth_m = parse_float(casing_depth_text)
        borehole_diameter_inches = parse_float(borehole_diameter_text)
        borehole_depth_m = parse_float(borehole_depth_text)
        mud_weight_equivalent_g_cm3 = parse_float(mud_weight_text)

        data = {
            'wlbwellborename': wlbwellborename,
            'casing_type': casing_type,
            'casing_diameter_inches': casing_diameter_inches,
            'casing_depth_m': casing_depth_m,
            'borehole_diameter_inches': borehole_diameter_inches,
            'borehole_depth_m': borehole_depth_m,
            'mud_weight_equivalent_g_cm3': mud_weight_equivalent_g_cm3,
            'test_type': test_type
        }

        # Upsert the data
        try:
            response = supabase.table('casing_and_tests') \
                .upsert(data, on_conflict='wlbwellborename,casing_type,casing_depth_m') \
                .execute()
            logging.info(f"Upserted casing and tests data for {wlbwellborename} casing type {casing_type} at depth {casing_depth_m}")
        except Exception as e:
            logging.error(f"Exception during database operation for {wlbwellborename}: {e}")
