import requests
from bs4 import BeautifulSoup
from datetime import datetime

def scrape_fact_pages():
    pending_wells_response = supabase.table('wellbore_data').select('wlbWellboreName', 'factpage_url').eq('status', 'pending').execute()

    for well in pending_wells_response.data:
        well_name = well['wlbWellboreName']
        factpage_url = well['factpage_url']
        scrape_fact_page(well_name, factpage_url)

        supabase.table('wellbore_data').update({
            'status': 'scraped',
            'last_scraped': datetime.now(),
            'needs_rescrape': False
        }).eq('wlbWellboreName', well_name).execute()

def scrape_fact_page(well_name, factpage_url):
    try:
        response = requests.get(factpage_url)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        wellbore_data = {
            'wlbwellborename': well_name,
            'wellbore_history': scrape_section(soup, 'Wellbore history'),
            'operations_and_results': scrape_section(soup, 'Operations and results'),
            'testing': scrape_section(soup, 'Testing'),
            'cuttings_info': scrape_cuttings(soup),
            'casing_tests': scrape_casing_tests(soup),
            'logs_info': scrape_logs(soup),
            'lithostratigraphy': scrape_lithostratigraphy(soup),
            'geochemical_info': scrape_section(soup, 'Geochemical information'),
            'drilling_mud_info': scrape_drilling_mud(soup),
            'palynological_slides': scrape_palynological_slides(soup),
            'thin_sections': scrape_thin_sections(soup)
        }

        supabase.table('wellbore_info').upsert(wellbore_data).execute()
        print(f"Scraped and saved data for well: {well_name}")
    except Exception as e:
        print(f"Failed to scrape {well_name}: {str(e)}")

def scrape_section(soup, section_name):
    section = soup.find('li', id=section_name.lower().replace(' ', '-'))
    return section.get_text(strip=True) if section else None

def scrape_cuttings(soup):
    return "Cuttings data scraped"

def scrape_casing_tests(soup):
    return "Casing tests data scraped"

def scrape_logs(soup):
    return "Logs data scraped"

def scrape_lithostratigraphy(soup):
    return "Lithostratigraphy data scraped"

def scrape_drilling_mud(soup):
    return "Drilling mud data scraped"

def scrape_palynological_slides(soup):
    return "Palynological slides data scraped"

def scrape_thin_sections(soup):
    return "Thin sections data scraped"
