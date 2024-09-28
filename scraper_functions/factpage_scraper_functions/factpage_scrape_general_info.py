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
            response.encoding = response.apparent_encoding
            html_content = response.text
            logging.info(f"Successfully fetched factpage for {wlbwellborename}")
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

    # Search for the general info section
    general_info_table = None
    for table in soup.find_all('table'):
        # Check if the table contains a cell with text 'Brønnbane navn'
        if table.find('td', string=lambda text: text and 'Brønnbane navn' in text):
            general_info_table = table
            logging.info(f"Found general info table for {wlbwellborename}")
            break

    if not general_info_table:
        logging.warning(f"'Brønnbane navn' section not found for {wlbwellborename}")
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
        'Hovedområde': 'hovedområde',
        'Funn': 'funn',
        'Brønn navn': 'bronn_navn',
        'Seismisk lokalisering': 'seismisk_lokalisering',
        'Utvinningstillatelse': 'utvinningstillatelse',
        'Boretillatelse': 'boretillatelse',
        'Boreoperatør': 'boreoperatoer',
        'Boreinnretning': 'boreinnretning',
        'Boredager': 'boredager',
        'Borestart': 'borestart',
        'Boreslutt': 'boreslutt',
        'Frigitt dato': 'frigitt_dato',
        'Publiseringsdato': 'publiseringsdato',
        'Opprinnelig formål': 'opprinnelig_formaal',
        'Gjenåpnet': 'gjenapnet',
        'Innhold': 'innhold',
        'Funnbrønnbane': 'funnbronnbane',
        'Avstand, boredekk - midlere havflate [m]': 'avstand_boredekk_m_m',
        'Vanndybde ved midlere havflate [m]': 'vanndybde_m_m',
        'Totalt målt dybde (MD) [m RKB]': 'totalt_maalt_dybde_md_m_rkb',
        'Totalt vertikalt dybde (TVD) [m RKB]': 'totalt_vertikalt_dybde_tvd_m_rkb',
        'Maks inklinasjon [°]': 'maks_inklinasjon_deg',
        'Temperatur ved bunn av brønnbanen [°C]': 'temperatur_bunn_bronnbane_c',
        'Eldste penetrerte alder': 'eldste_penetrerte_alder',
        'Eldste penetrerte formasjon': 'eldste_penetrerte_formasjon',
        'Geodetisk datum': 'geodetisk_datum',
        'NS grader': 'ns_grader',
        'ØV grader': 'ov_grader',
        'NS UTM [m]': 'ns_utm_m',
        'ØV UTM [m]': 'ov_utm_m',
        'UTM sone': 'utm_sone',
        'NPDID for brønnbanen': 'npdid_bronnbanen'
    }

    # Function to parse date strings into YYYY-MM-DD format
    def parse_date(date_str):
        try:
            return datetime.strptime(date_str, '%d.%m.%Y').strftime('%Y-%m-%d')
        except ValueError:
            logging.warning(f"Failed to parse date: '{date_str}'")
            return None

    # Iterate over the table rows to extract label-value pairs
    for row in general_info_table.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) != 2:
            logging.debug(f"Skipping row with unexpected number of cells: {len(cells)}")
            continue  # Skip rows that don't have exactly two cells

        # Extract only the direct text from the first cell (label)
        label_cell = cells[0]
        # Use .contents to get direct children and extract text without child elements
        label_text = ''.join([str(item) for item in label_cell.contents if isinstance(item, str)]).strip().rstrip(':')
        
        if not label_text:
            # Fallback: get all text and strip unwanted parts
            label_text = label_cell.get_text(separator=' ', strip=True).rstrip(':')
        
        value_cell = cells[1]
        # Similarly, extract only the direct text, excluding child elements like buttons
        value_text = ''.join([str(item) for item in value_cell.contents if isinstance(item, str)]).strip()
        if not value_text:
            # Fallback: get all text
            value_text = value_cell.get_text(separator=' ', strip=True)
        
        logging.debug(f"Extracted Label: '{label_text}', Value: '{value_text}'")

        if label_text in label_to_column:
            column = label_to_column[label_text]
            if column in ['borestart', 'boreslutt', 'frigitt_dato', 'publiseringsdato']:
                parsed_date = parse_date(value_text)
                if parsed_date:
                    data[column] = parsed_date
                else:
                    logging.warning(f"Failed to parse date for {wlbwellborename}, field '{column}': '{value_text}'")
            else:
                data[column] = value_text
        else:
            logging.warning(f"Label '{label_text}' not found in label_to_column mapping for {wlbwellborename}")

    # Log the final data dictionary
    logging.debug(f"Final Data for {wlbwellborename}: {data}")

    # Upsert the data into the 'general_info' table
    try:
        response = supabase.table('general_info') \
            .upsert(data, on_conflict='wlbwellborename') \
            .execute()
        
        if response.status_code < 400:
            logging.info(f"Successfully upserted general info for well '{wlbwellborename}'")
        else:
            logging.error(f"Failed to upsert general info for well '{wlbwellborename}': {response.status_code} - {response.status_message}")
    except Exception as e:
        logging.error(f"Exception during upsert for {wlbwellborename}: {e}")
