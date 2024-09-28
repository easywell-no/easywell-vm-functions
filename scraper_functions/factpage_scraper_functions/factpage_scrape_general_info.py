import requests
from bs4 import BeautifulSoup
from supabase import Client
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import time

def scrape_general_info(supabase: Client, wlbwellborename: str, factpage_url: str):
    max_retries = 3
    html_content = None

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
        except (HTTPError, ConnectionError, Timeout, RequestException):
            if attempt < max_retries - 1:
                time.sleep(2)  # Wait before retrying
            else:
                return  # Exit if all retries fail

    if not html_content:
        return  # Exit if no HTML content retrieved

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
            return datetime.strptime(date_str, '%d.%m.%Y').strftime('%Y-%m-%d')
        except ValueError:
            return None

    # Iterate over all tables and extract label-value pairs
    for table in soup.find_all('table'):
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) != 2:
                continue

            label_cell = cells[0]
            label_text = label_cell.get_text(separator=' ', strip=True).rstrip(':')
            value_cell = cells[1]
            value_text = value_cell.get_text(separator=' ', strip=True)

            if label_text in label_to_column:
                column = label_to_column[label_text]
                if column in ['borestart', 'boreslutt', 'frigitt_dato', 'publiseringsdato']:
                    data[column] = parse_date(value_text)
                elif column in ['gjenapnet', 'funnbronnbane']:
                    data[column] = True if value_text.strip().upper() == "YES" else False
                elif column in ['boredager', 'maks_inklinasjon_deg', 'utm_sone', 'npdid_bronnbanen']:
                    try:
                        data[column] = int(value_text)
                    except ValueError:
                        data[column] = None
                elif column in ['avstand_boredekk_m_m', 'vanndybde_m_m',
                               'totalt_maalt_dybde_md_m_rkb', 'totalt_vertikalt_dybde_tvd_m_rkb',
                               'temperatur_bunn_bronnbane_c', 'ns_utm_m', 'ov_utm_m']:
                    try:
                        data[column] = float(value_text.replace(',', '.'))
                    except ValueError:
                        data[column] = None
                else:
                    data[column] = value_text

    # Upsert the data into the 'general_info' table
    try:
        supabase.table('general_info') \
            .upsert(data, on_conflict='wlbwellborename') \
            .execute()
    except Exception as e:
        pass  # Handle errors as necessary
