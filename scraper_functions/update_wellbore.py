import pandas as pd
from datetime import datetime
from supabase import create_client, Client
import os

# Define your Supabase credentials
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

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
        if key in ['last_scraped', 'date_last_updated_csv', 'wlbentrydate',
                   'wlbcompletiondate', 'wlbentrypredrilldate', 'wlbcomppredrilldate',
                   'wlbwdssqcdate', 'wlbreleaseddate', 'wlbdatereclass',
                   'wlbpluggedabandondate', 'wlbpluggeddate']:
            if value:
                try:
                    row[key] = pd.to_datetime(value).date()
                except:
                    row[key] = None
            else:
                row[key] = None
        elif key in [
            'wlbbottomholetemperature', 'wlbmaxinclation', 'wlbkellybushelevation',
            'wlbfinalverticaldepth', 'wlbtotaldepth', 'wlbwaterdepth',
            'wlbnsdeg', 'wlbnsmin', 'wlbnssec', 'wlbewdeg', 'wlbewmin',
            'wlbewsec', 'wlbnsdecdeg', 'wlbewdecdeg'
        ]:
            if value is not None:
                try:
                    row[key] = float(value)
                except:
                    row[key] = None
        elif key in [
            'wlbsubsea', 'wlbdiscoverywellbore', 'wlbmultilateral', 
            'wlbreentry'
        ]:
            if isinstance(value, bool):
                pass
            elif isinstance(value, str):
                row[key] = value.lower() in ['true', '1', 'yes']
            else:
                row[key] = bool(value)
        elif key in ['wlbentryyear', 'wlbcompletionyear', 'wlbdrillingdays']:
            if value is not None:
                try:
                    row[key] = int(value)
                except:
                    row[key] = None
        # Add more type conversions as needed
    return row

def update_wellbore_data(supabase_client):
    print("Starting daily update of wellbore_data at:", datetime.now())

    new_wells_count = 0
    updated_wells_count = 0
    deleted_wells_count = 0

    # Load the CSV data from Sokkeldirektoratet
    csv_url = 'https://factpages.sodir.no/public?/Factpages/external/tableview/wellbore_all_long&rs:Command=Render&rc:Toolbar=false&rc:Parameters=f&IpAddress=not_used&CultureCode=nb-no&rs:Format=CSV&Top100=false'
    df = pd.read_csv(csv_url, delimiter=",")

    # Normalize data to avoid empty values causing errors
    df = df.applymap(normalize_value)

    # Map CSV columns to SQL columns if necessary
    df.rename(columns=CSV_TO_SQL_MAPPING, inplace=True)

    # Ensure all Supabase columns are present in DataFrame
    for col in SUPABASE_COLUMNS:
        if col not in df.columns:
            df[col] = None  # or set a default value if appropriate

    # Select only the columns relevant to Supabase
    df = df[SUPABASE_COLUMNS]

    # Convert data types as per Supabase schema
    df = df.apply(convert_types, axis=1)

    # Get all existing wells in the database
    existing_wells_response = supabase_client.table('wellbore_data').select('*').execute()
    if existing_wells_response.error:
        print("Error fetching existing wells:", existing_wells_response.error)
        return
    existing_wells = {well['wlbwellborename']: well for well in existing_wells_response.data}

    # Collect batch data for insert and update
    new_wells = []
    wells_to_update = []

    for index, row in df.iterrows():
        well_name = row['wlbwellborename']
        date_updated_csv = row.get('wlbdateupdatedmax')

        # Convert the row to a dictionary
        well_data = row.to_dict()

        # Add processing columns
        if well_name in existing_wells:
            current_well = existing_wells[well_name]
            # Determine if any relevant columns have changed
            changed = False
            for col in SUPABASE_COLUMNS:
                if col in IGNORED_COLUMNS:
                    continue
                # Compare current value in DB with the new value
                new_val = well_data.get(col)
                current_val = current_well.get(col)
                if new_val != current_val:
                    changed = True
                    break

            if changed:
                print(f"Well '{well_name}' has updates. Marking for re-scraping.")
                well_data['status'] = 'pending'
                well_data['needs_rescrape'] = True
                well_data['last_scraped'] = None  # Reset last_scraped if needed
                wells_to_update.append(well_data)
        else:
            # Add new wells to the list
            print(f"New well found: '{well_name}'. Adding to database.")
            well_data['status'] = 'never'
            well_data['needs_rescrape'] = False
            well_data['last_scraped'] = None
            new_wells.append(well_data)

    # Perform batch insert of new wells
    if new_wells:
        insert_response = supabase_client.table('wellbore_data').insert(new_wells).execute()
        if insert_response.error:
            print("Error inserting new wells:", insert_response.error)
        else:
            new_wells_count = len(new_wells)

    # Perform batch update of existing wells
    if wells_to_update:
        # Supabase does not support batch updates directly.
        # You might need to update each well individually or use another approach.
        for well_data in wells_to_update:
            update_response = supabase_client.table('wellbore_data').update(well_data).eq('wlbwellborename', well_data['wlbwellborename']).execute()
            if update_response.error:
                print(f"Error updating well '{well_data['wlbwellborename']}':", update_response.error)
            else:
                updated_wells_count += 1

    # Detect wells that were deleted from the CSV but still exist in the database
    csv_well_names = set(df['wlbwellborename'].dropna().unique())
    existing_well_names = set(existing_wells.keys())
    deleted_wells = existing_well_names - csv_well_names
    if deleted_wells:
        deleted_wells_count = len(deleted_wells)
        print(f"{deleted_wells_count} wells have been removed from the CSV but will be retained in the database.")
        # Optionally, handle deletions (e.g., mark as inactive)
        # Example:
        # for well_name in deleted_wells:
        #     supabase_client.table('wellbore_data').update({'status': 'inactive'}).eq('wlbwellborename', well_name).execute()

    print(f"Update completed at {datetime.now()}. New: {new_wells_count}, Updated: {updated_wells_count}, Deleted: {deleted_wells_count}")

if __name__ == "__main__":
    update_wellbore_data(supabase)