import requests
from bs4 import BeautifulSoup
import logging
from supabase import Client
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import time
from datetime import datetime

# Function to extract data from a single well page
def scrape_general_info(supabase: Client, wlbwellborename: str, factpage_url: str):
    max_retries = 3
    html_content = None

    try:
        supabase.table('general_info')\
            .delete()\
            .eq('wlbwellborename', wlbwellborename)\
            .execute()
        logging.info(f"Deleted existing general info data for {wlbwellborename}")
    except Exception as e:
        logging.error(f"Failed to delete existing general info data for {wlbwellborename}: {e}")

    # Retry mechanism for fetching the page
    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0',
                'Accept-Language': 'no'
            }
            response = requests.get(factpage_url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = response.apparent_encoding  # Correct encoding
            html_content = response.text
            break
        except (HTTPError, ConnectionError, Timeout, RequestException) as e:
            logging.error(f"Error fetching the factpage for {wlbwellborename}: {e}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying... ({attempt + 1}/{max_retries})")
                time.sleep(2)  # Wait before retrying
            else:
                logging.error(f"Failed to fetch factpage after {max_retries} attempts for {wlbwellborename}")
                return

    if not html_content:
        logging.error(f"No HTML content retrieved for {wlbwellborename}")
        return

    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # Initialize the data dictionary
        data = {'wlbwellborename': wlbwellborename.strip()}

        # Label to column mapping
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

        # Helper function to parse date strings
        def parse_date(date_str):
            try:
                return datetime.strptime(date_str.strip(), '%d.%m.%Y').strftime('%Y-%m-%d')
            except ValueError:
                return None

        # Function to extract values based on a specific keyword in the table row
        def extract_value(soup, keyword):
            rows = soup.find_all('tr')  # Find all table rows
            for row in rows:
                cells = row.find_all('td')
                if cells and len(cells) >= 2:
                    cell_text = cells[0].get_text(strip=True)
                    if keyword in cell_text:
                        return cells[1].get_text(strip=True)
            return None

        # Iterate through the label-to-column mappings and extract values
        for label, column in label_to_column.items():
            value = extract_value(soup, label)
            if value:
                if column in ['borestart', 'boreslutt', 'frigitt_dato', 'publiseringsdato']:
                    data[column] = parse_date(value)
                elif column in ['gjenapnet', 'funnbronnbane']:
                    data[column] = value.strip().upper() == "YES"
                elif column in ['boredager', 'maks_inklinasjon_deg', 'utm_sone', 'npdid_bronnbanen']:
                    try:
                        data[column] = int(value.replace(',', '').replace(' ', ''))
                    except ValueError:
                        data[column] = None
                elif column in ['avstand_boredekk_m_m', 'vanndybde_m_m',
                               'totalt_maalt_dybde_md_m_rkb', 'totalt_vertikalt_dybde_tvd_m_rkb',
                               'temperatur_bunn_bronnbane_c', 'ns_utm_m', 'ov_utm_m']:
                    try:
                        data[column] = float(value.replace(',', '.').replace(' ', ''))
                    except ValueError:
                        data[column] = None
                else:
                    data[column] = value

        # Log the data before upserting
        logging.debug(f"Data to be upserted for {wlbwellborename}: {data}")

        # Upsert the data into the 'general_info' table
        try:
            result = supabase.table('general_info').upsert(data, on_conflict='wlbwellborename').execute()
            logging.info(f"Upserted data for {wlbwellborename}")
        except Exception as e:
            logging.error(f"Error upserting data for {wlbwellborename}: {e}")
    except Exception as e:
        logging.error(f"Exception during scraping general info for {wlbwellborename}: {e}", exc_info=True)

    # Return the scraped data for debugging purposes
    return data
