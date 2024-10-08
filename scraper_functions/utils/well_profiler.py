# utils/well_profiler.py

import logging

def get_well_profiles(well_names, supabase):
    """
    Retrieve and aggregate data for the specified wells from multiple tables.

    Args:
        well_names (list): A list of well names (wlbwellborename).
        supabase (Client): The Supabase client instance.

    Returns:
        dict: A dictionary containing well profiles for each well.
    """
    logging.info("Fetching well profiles for the specified wells.")
    
    # Initialize the well_profiles dictionary
    well_profiles = {well_name: {} for well_name in well_names}
    
    # List of tables to query and the key to store their data under
    tables = [
        ('wellbore_data', 'wellbore_data'),
        ('general_info', 'general_info'),
        ('wellbore_history', 'wellbore_history'),
        ('lithostratigraphy', 'lithostratigraphy'),
        ('casing_and_tests', 'casing_and_tests'),
        ('drilling_fluid', 'drilling_fluid')
    ]
    
    for table_name, data_key in tables:
        try:
            # Fetch data from the table for all well names
            response = supabase.table(table_name).select('*').in_('wlbwellborename', well_names).execute()
            records = response.data
            logging.info(f"Fetched {len(records)} records from '{table_name}' table.")
            
            # Organize data per well
            for record in records:
                well_name = record.get('wlbwellborename')
                if well_name in well_profiles:
                    # Initialize the list for tables that may have multiple records per well
                    if data_key in ['wellbore_history', 'lithostratigraphy', 'casing_and_tests', 'drilling_fluid']:
                        if data_key not in well_profiles[well_name]:
                            well_profiles[well_name][data_key] = []
                        well_profiles[well_name][data_key].append(record)
                    else:
                        # For tables with one record per well
                        well_profiles[well_name][data_key] = record
                else:
                    logging.warning(f"Well name '{well_name}' from '{table_name}' not found in initial well list.")
        except Exception as e:
            logging.error(f"Error fetching data from '{table_name}': {e}")
            continue
        
    logging.info("Completed fetching well profiles.")
    return well_profiles
