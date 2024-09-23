import pandas as pd
from datetime import datetime
from supabase import Client
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("update_wellbore.log"),
        logging.StreamHandler()
    ]
)

# Columns to ignore when checking for updates
IGNORED_COLUMNS = [
    'datesyncNPD', 'last_scraped', 'status', 
    'needs_rescrape', 'date_last_updated_csv'
]

# Mapping from CSV columns to SQL columns (if names differ)
CSV_TO_SQL_MAPPING = {
    'wlbWellboreName': 'wlbwellborename',
    'wlbFactPageUrl': 'wlbfactpageurl',
    'wlbDateUpdatedMax': 'wlbdateupdatedmax',
    # Add additional mappings if column names differ
}

# List of all Supabase table columns
SUPABASE_COLUMNS = [
    'wlbwellborename', 'last_scraped', 'status', 
    'needs_rescrape', 'date_last_updated_csv', 'wlbwelltype', 
    'wlbwell', 'wlbdrillingoperator', 'wlbproductionlicence', 
    'wlbpurpose', 'wlbstatus', 'wlbcontent', 'wlbsubsea', 
    'wlbentrydate', 'wlbcompletiondate', 'wlbentrypredrilldate', 
    'wlbcomppredrilldate', 'wlbfield', 'wlbdrillpermit', 
    'wlbdiscovery', 'wlbdiscoverywellbore', 'wlbbottomholetemperature', 
    'wlbsitesurvey', 'wlbseismiclocation', 'wlbmaxinclation', 
    'wlbkellybushelevation', 'wlbfinalverticaldepth', 'wlbtotaldepth', 
    'wlbwaterdepth', 'wlbkickoffpoint', 'wlbageattd', 
    'wlbformationattd', 'wlbmainarea', 'wlbdrillingfacility', 
    'wlbfacilitytypedrilling', 'wlbdrillingfacilityfixedormoveable', 
    'wlbproductionfacility', 'wlblicensingactivity', 'wlbmultilateral', 
    'wlbpurposeplanned', 'wlbcontentplanned', 'wlbentryyear', 
    'wlbcompletionyear', 'wlbreclassfromwellbore', 
    'wlbreentryexplorationactivity', 'wlbplotsymbol', 
    'wlbformationwithhc1', 'wlbagewithhc1', 'wlbformationwithhc2', 
    'wlbagewithhc2', 'wlbformationwithhc3', 'wlbagewithhc3', 
    'wlbdrillingdays', 'wlbreentry', 'wlblicencetargetname', 
    'wlbpluggedabandondate', 'wlbpluggeddate', 'wlbgeodeticdatum', 
    'wlbnsdeg', 'wlbnsmin', 'wlbnssec', 'wlbnscode', 'wlbewdeg', 
    'wlbewmin', 'wlbewsec', 'wlbewcode', 'wlbnsdecdeg', 
    'wlbewdecdeg', 'wlbnsutm', 'wlbewutm', 'wlbutmzone', 
    'wlbnamepart1', 'wlbnamepart2', 'wlbdiskoswellboretype', 
    'wlbnamepart3', 'wlbnamepart4', 'wlbnamepart5', 
    'wlbnamepart6', 'wlbpressreleaseurl', 'wlbfactpageurl', 
    'wlbfactmapurl', 'wlbdiskoswellboretype1', 
    'wlbdiskoswellboreparent', 'wlbwdssqcdate', 'wlbreleaseddate', 
    'wlbdatereclass', 'wlbnpdidwellbore', 
    'prlnpdidprodlicencetarget', 'fclnpdidfacilityproducing', 
    'dscnpdiddiscovery', 'fldnpdidfield', 'fclnpdidfacilitydrilling', 
    'wlbnpdidwellborereclass', 'prlnpdidproductionlicence', 
    'wlbnpdidsitesurvey', 'wlbaliasname', 'wlbdateupdated', 
    'wlbdateupdatedmax', 'datesyncnpd'
    # Add any additional columns if necessary
]

def normalize_value(value):
    """Converts empty values to None."""
    if pd.isnull(value) or value == '':
        return None
    return value

def convert_types(row):
    """
    Convert row values to appropriate types based on Supabase schema.
    Adjust this function as needed to handle specific conversions.
    """
    for key, value in row.items():
        # Handle dates
        if key in [
            'last_scraped', 'date_last_updated_csv', 'wlbentrydate',
            'wlbcompletiondate', 'wlbentrypredrilldate', 'wlbcomppredrilldate',
            'wlbwdssqcdate', 'wlbreleaseddate', 'wlbdatereclass',
            'wlbpluggedabandondate', 'wlbpluggeddate'
        ]:
            try:
                row[key] = pd.to_datetime(value, dayfirst=True).date() if value else None
            except Exception as e:
                logging.warning(f"Date conversion error for {key}: {e}")
                row[key] = None
        # Handle floats
        elif key in [
            'wlbbottomholetemperature', 'wlbmaxinclation', 'wlbkellybushelevation',
            'wlbfinalverticaldepth', 'wlbtotaldepth', 'wlbwaterdepth',
            'wlbnsdeg', 'wlbnsmin', 'wlbnssec', 'wlbewdeg', 'wlbewmin',
            'wlbewsec', 'wlbnsdecdeg', 'wlbewdecdeg'
        ]:
            try:
                row[key] = float(value) if value is not None else None
            except Exception as e:
                logging.warning(f"Float conversion error for {key}: {e}")
                row[key] = None
        # Handle booleans
        elif key in ['wlbsubsea', 'wlbdiscoverywellbore', 'wlbmultilateral', 'wlbreentry']:
            if isinstance(value, bool):
                pass
            elif isinstance(value, str):
                row[key] = value.lower() in ['true', '1', 'yes']
            else:
                row[key] = bool(value)
        # Handle integers
        elif key in ['wlbentryyear', 'wlbcompletionyear', 'wlbdrillingdays']:
            try:
                row[key] = int(value) if value is not None else None
            except Exception as e:
                logging.warning(f"Integer conversion error for {key}: {e}")
                row[key] = None
    return row

def update_wellbore_data(supabase_client: Client):
    logging.info("Starting daily update of wellbore_data")
    
    try:
        # Load the CSV data from Sokkeldirektoratet
        csv_url = 'https://factpages.sodir.no/public?/Factpages/external/tableview/wellbore_all_long&rs:Command=Render&rc:Toolbar=false&rc:Parameters=f&IpAddress=not_used&CultureCode=nb-no&rs:Format=CSV&Top100=false'
        df = pd.read_csv(csv_url, delimiter="\t")  # Corrected delimiter
        logging.info("CSV data loaded successfully.")
    except Exception as e:
        logging.error(f"Failed to load CSV data: {e}")
        return

    try:
        # Normalize data to avoid empty values causing errors
        df = df.applymap(normalize_value)  # Using applymap
        logging.info("Data normalization completed.")
    except Exception as e:
        logging.error(f"Data normalization failed: {e}")
        return

    try:
        # Map CSV columns to SQL columns if necessary
        df.rename(columns=CSV_TO_SQL_MAPPING, inplace=True)
        logging.info("Column mapping completed.")
    except Exception as e:
        logging.error(f"Column mapping failed: {e}")
        return

    try:
        # Ensure all Supabase columns are present in DataFrame
        for col in SUPABASE_COLUMNS:
            if col not in df.columns:
                df[col] = None  # or set a default value if appropriate
        logging.info("Ensured all Supabase columns are present.")
    except Exception as e:
        logging.error(f"Ensuring Supabase columns failed: {e}")
        return

    try:
        # Select only the columns relevant to Supabase
        df = df[SUPABASE_COLUMNS]
        logging.info("Selected relevant Supabase columns.")
    except Exception as e:
        logging.error(f"Selecting Supabase columns failed: {e}")
        return

    try:
        # Convert data types as per Supabase schema
        df = df.apply(convert_types, axis=1)
        logging.info("Data type conversion completed.")
    except Exception as e:
        logging.error(f"Data type conversion failed: {e}")
        return

    try:
        # Get all existing wells in the database
        existing_wells_response = supabase_client.table('wellbore_data').select('*').execute()
        if existing_wells_response.status_code != 200:
            logging.error(f"Error fetching existing wells: {existing_wells_response.status_code}")
            return
        existing_wells = {well['wlbwellborename']: well for well in existing_wells_response.data}
        logging.info("Fetched existing wells successfully.")
    except Exception as e:
        logging.error(f"Error processing existing wells: {e}")
        return

    # Collect batch data for insert and update
    new_wells = []
    wells_to_update = []

    try:
        for index, row in df.iterrows():
            well_name = row['wlbwellborename']
            date_updated_csv = row.get('wlbdateupdatedmax')

            # Convert the row to a dictionary
            well_data = row.to_dict()

            if well_name in existing_wells:
                current_well = existing_wells[well_name]
                # Determine if any relevant columns have changed
                changed = any(well_data.get(col) != current_well.get(col) for col in SUPABASE_COLUMNS if col not in IGNORED_COLUMNS)

                if changed:
                    logging.info(f"Well '{well_name}' has updates. Marking for re-scraping.")
                    well_data['status'] = 'pending'
                    well_data['needs_rescrape'] = True
                    well_data['last_scraped'] = None
                    wells_to_update.append(well_data)
            else:
                # Add new wells to the list
                logging.info(f"New well found: '{well_name}'. Adding to database.")
                well_data['status'] = 'never'
                well_data['needs_rescrape'] = False
                well_data['last_scraped'] = None
                new_wells.append(well_data)
    except Exception as e:
        logging.error(f"Error processing wells for insert/update: {e}")
        return

    # Perform batch insert of new wells
    try:
        if new_wells:
            insert_response = supabase_client.table('wellbore_data').insert(new_wells).execute()
            if insert_response.status_code != 200:
                logging.error(f"Error inserting new wells: {insert_response.status_code}")
            else:
                new_wells_count = len(new_wells)
                logging.info(f"Inserted {new_wells_count} new wells successfully.")
    except Exception as e:
        logging.error(f"Error during batch insert: {e}")

    # Perform batch update of existing wells
    try:
        if wells_to_update:
            for well_data in wells_to_update:
                update_response = supabase_client.table('wellbore_data').update(well_data).eq('wlbwellborename', well_data['wlbwellborename']).execute()
                if update_response.status_code != 200:
                    logging.error(f"Error updating well '{well_data['wlbwellborename']}': {update_response.status_code}")
                else:
                    updated_wells_count += 1
            logging.info(f"Updated {updated_wells_count} wells successfully.")
    except Exception as e:
        logging.error(f"Error during batch update: {e}")

    # Detect wells that were deleted from the CSV but still exist in the database
    try:
        csv_well_names = set(df['wlbwellborename'].dropna().unique())
        existing_well_names = set(existing_wells.keys())
        deleted_wells = existing_well_names - csv_well_names
        if deleted_wells:
            deleted_wells_count = len(deleted_wells)
            logging.info(f"{deleted_wells_count} wells removed from the CSV but retained in the database.")
            # Optionally, handle deletions (e.g., mark as inactive)
            # Example:
            # for well_name in deleted_wells:
            #     supabase_client.table('wellbore_data').update({'status': 'inactive'}).eq('wlbwellborename', well_name).execute()
    except Exception as e:
        logging.error(f"Error detecting deleted wells: {e}")

    logging.info(f"Update completed at {datetime.now()}. New: {new_wells_count}, Updated: {updated_wells_count}, Deleted: {deleted_wells_count}")
