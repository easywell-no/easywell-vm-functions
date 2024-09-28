import logging
from bs4 import BeautifulSoup
import requests
from supabase import Client
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import time
import re

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

    # Find the 'Generell informasjon' table
    general_info_table = soup.find('table', class_='general-info-table')
    if not general_info_table:
        logging.warning(f"'general-info-table' not found for {wlbwellborename}")
        return
    else:
        logging.info(f"Found 'general-info-table' for {wlbwellborename}")

    # Initialize a dictionary to hold the scraped data
    data = {
        'wlbwellborename': wlbwellborename
    }

    # Define a mapping from the HTML labels to database column names
    label_to_column = {
        'Brønnbane navn': 'bronnbane_navn',
        'Type': 'type',
        'Formål': 'formål',
        'Status': 'status',
        'Faktakart i nytt vindu': 'faktakart_i_nytt_vindu',
        'lenke til kart': 'lenke_til_kart',
        'Hovedområde': 'hovedområde',
        'Felt': 'felt',
        'Funn': 'funn',
        'Brønn navn': 'brønn_navn',
        'Seismisk lokalisering': 'seismisk_lokalisering',
        'Utvinningstillatelse': 'utvinningstillatelse',
        'Boreoperatør': 'boreoperatør',
        'Boretillatelse': 'boretillatelse',
        'Boreinnretning': 'boreinnretning',
        'Boredager': 'boredager',
        'Borestart': 'borestart',
        'Boreslutt': 'boreslutt',
        'Frigitt dato': 'frigitt_dato',
        'Publiseringsdato': 'publiseringsdato',
        'Opprinnelig formål': 'opprinnelig_formål',
        'Reklassifisert til brønnbane': 'reklassifisert_til_bronnbane',
        'Gjenåpnet': 'gjenåpnet',
        'Innhold': 'innhold',
        'Funnbrønnbane': 'funnbronnbane',
        '1. nivå med hydrokarboner, alder': 'nivå_hydrokarboner_alder',
        '1. nivå med hydrokarboner, formasjon.': 'nivå_hydrokarboner_formasjon',
        'Avstand, boredekk - midlere havflate [m]': 'avstand_boredekk_mid_havflate_m',
        'Vanndybde ved midlere havflate [m]': 'vanndybde_mid_havflate_m',
        'Totalt målt dybde (MD) [m RKB]': 'totalt_maalt_dybde_md_m_rkb',
        'Totalt vertikalt dybde (TVD) [m RKB]': 'totalt_vertikalt_dybde_tvd_m_rkb',
        'Maks inklinasjon [°]': 'maks_inklinasjon',
        'Temperatur ved bunn av brønnbanen [°C]': 'temperatur_bunn_brønnbanen_c',
        'Eldste penetrerte alder': 'eldste_penetrerte_alder',
        'Eldste penetrerte formasjon': 'eldste_penetrerte_formasjon',
        'Geodetisk datum': 'geodetisk_datum',
        'NS grader': 'ns_grader',
        'ØV grader': 'ov_grader',
        'NS UTM [m]': 'ns_utm_m',
        'ØV UTM [m]': 'ov_utm_m',
        'UTM sone': 'utm_sone',
        'NPDID for brønnbane': 'npdid_for_bronnbane'
    }

    # Function to parse date strings into YYYY-MM-DD format
    def parse_date(date_str):
        try:
            return time.strftime('%Y-%m-%d', time.strptime(date_str, '%d.%m.%Y'))
        except ValueError:
            logging.warning(f"Invalid date format '{date_str}' for {wlbwellborename}")
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
            # Special handling for certain fields
            if column in ['borestart', 'boreslutt', 'frigitt_dato', 'publiseringsdato']:
                data[column] = parse_date(value)
            elif column == 'boredager':
                try:
                    data[column] = int(value)
                except ValueError:
                    logging.warning(f"Invalid integer value '{value}' for {column} in {wlbwellborename}")
                    data[column] = None
            elif column in ['avstand_boredekk_mid_havflate_m', 'vanndybde_mid_havflate_m',
                           'totalt_maalt_dybde_md_m_rkb', 'totalt_vertikalt_dybde_tvd_m_rkb',
                           'maks_inklinasjon', 'temperatur_bunn_brønnbanen_c',
                           'ns_utm_m', 'ov_utm_m']:
                try:
                    data[column] = float(value.replace(',', '.'))
                except ValueError:
                    logging.warning(f"Invalid float value '{value}' for {column} in {wlbwellborename}")
                    data[column] = None
            elif column in ['utm_sone', 'npdid_for_bronnbane']:
                try:
                    data[column] = int(value)
                except ValueError:
                    logging.warning(f"Invalid integer value '{value}' for {column} in {wlbwellborename}")
                    data[column] = None
            else:
                data[column] = value
        else:
            logging.debug(f"Unmapped label '{label}' encountered for {wlbwellborename}")

    # Check if mandatory fields are present
    if 'wlbwellborename' not in data or not data['wlbwellborename']:
        logging.error(f"'wlbwellborename' missing or empty for {wlbwellborename}, skipping insertion.")
        return

    # Replace any missing data with None
    for key in label_to_column.values():
        if key not in data:
            data[key] = None

    # Upsert the data into the 'general_info' table
    try:
        response = supabase.table('general_info') \
            .upsert(data, on_conflict='wlbwellborename') \
            .execute()
        if response.status_code in [200, 201]:
            logging.info(f"Upserted general_info data for {wlbwellborename}")
        else:
            logging.error(f"Failed to upsert general_info data for {wlbwellborename}: {response}")
    except Exception as e:
        logging.error(f"Exception during database operation for {wlbwellborename}: {e}")
