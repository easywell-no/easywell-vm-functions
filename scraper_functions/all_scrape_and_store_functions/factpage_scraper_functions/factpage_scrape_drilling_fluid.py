import requests
from bs4 import BeautifulSoup
import logging
from supabase import Client
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import time

def scrape_drilling_fluid(supabase: Client, wlbwellborename: str, factpage_url: str):
    max_retries = 3
    html_content = None

    # Delete existing data for the wellbore
    try:
        supabase.table('drilling_fluid')\
            .delete()\
            .eq('wlbwellborename', wlbwellborename)\
            .execute()
        logging.info(f"Deleted existing drilling fluid data for {wlbwellborename}")
    except Exception as e:
        logging.error(f"Failed to delete existing drilling fluid data for {wlbwellborename}: {e}")
    

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

    # Find the drilling fluid section
    fluid_section = soup.find('li', id='boreslam')
    if not fluid_section:
        logging.warning(f"'Boreslam' section not found for {wlbwellborename}")
        return
    else:
        logging.info(f"Found 'Boreslam' section for {wlbwellborename}")

    # Find the table within the section
    table = fluid_section.find('table', class_='a2414 uk-table-striped')
    if not table:
        logging.warning(f"No drilling fluid table found for {wlbwellborename}")
        return

    tbody = table.find('tbody')
    if not tbody:
        logging.warning(f"No table body found in drilling fluid table for {wlbwellborename}")
        return

    rows = tbody.find_all('tr')
    if not rows:
        logging.warning(f"No data rows found in drilling fluid table for {wlbwellborename}")
        return

    for row in rows:
        cells = row.find_all('td')
        if len(cells) != 6:
            logging.warning(f"Unexpected number of cells ({len(cells)}) in row for {wlbwellborename}")
            continue
        # Extract data from cells
        depth_cell = cells[0]
        mud_weight_cell = cells[1]
        mud_viscosity_cell = cells[2]
        yield_point_cell = cells[3]
        mud_type_cell = cells[4]
        measurement_date_cell = cells[5]

        # Get text and strip whitespace
        depth_text = depth_cell.get_text(strip=True)
        mud_weight_text = mud_weight_cell.get_text(strip=True)
        mud_viscosity_text = mud_viscosity_cell.get_text(strip=True)
        yield_point_text = yield_point_cell.get_text(strip=True)
        mud_type = mud_type_cell.get_text(strip=True)
        measurement_date_text = measurement_date_cell.get_text(strip=True)

        # Convert numeric values to floats, handle any parsing issues
        def parse_float(text):
            if not text:
                return None
            try:
                text = text.strip()
                value = float(text.replace(',', '.'))
                logging.debug(f"Parsed '{text}' as {value}")
                return value
            except ValueError:
                logging.warning(f"Invalid float value '{text}' for {wlbwellborename}")
                return None

        depth_m_md = parse_float(depth_text)
        mud_weight_g_cm3 = parse_float(mud_weight_text)
        mud_viscosity_mpa_s = parse_float(mud_viscosity_text)
        yield_point_pa = parse_float(yield_point_text)

        # Parse date if available
        measurement_date = None
        if measurement_date_text:
            try:
                # Assuming date format is 'dd.mm.yyyy'
                measurement_date = time.strptime(measurement_date_text, '%d.%m.%Y')
                measurement_date = time.strftime('%Y-%m-%d', measurement_date)
            except ValueError:
                logging.warning(f"Invalid date format '{measurement_date_text}' for {wlbwellborename}")
                measurement_date = None

        data = {
            'wlbwellborename': wlbwellborename,
            'depth_m_md': depth_m_md,
            'mud_weight_g_cm3': mud_weight_g_cm3,
            'mud_viscosity_mpa_s': mud_viscosity_mpa_s,
            'yield_point_pa': yield_point_pa,
            'mud_type': mud_type,
            'measurement_date': measurement_date
        }

        # Upsert the data
        try:
            response = supabase.table('drilling_fluid') \
                .upsert(data, on_conflict='wlbwellborename,depth_m_md') \
                .execute()
            logging.info(f"Upserted drilling fluid data for {wlbwellborename} at depth {depth_m_md}")
        except Exception as e:
            logging.error(f"Exception during database operation for {wlbwellborename}: {e}")
