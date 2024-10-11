# utils/well_profiler.py

import logging

def get_well_profiles(well_names, supabase):
    logging.info("Fetching well profiles for the specified wells.")
    
    # Initialize the well_profiles dictionary with all required keys
    well_profiles = {well_name: {
        'general_info': None,
        'wellbore_history': [],
        'lithostratigraphy': [],
        'casing_and_tests': [],
        'drilling_fluid': []
    } for well_name in well_names}
    
    tables = [
        ('general_info', 'general_info'),
        ('wellbore_history', 'wellbore_history'),
        ('lithostratigraphy', 'lithostratigraphy'),
        ('casing_and_tests', 'casing_and_tests'),
        ('drilling_fluid', 'drilling_fluid')
    ]
    
    for table_name, data_key in tables:
        try:
            response = supabase.table(table_name).select('*').in_('wlbwellborename', well_names).execute()
            records = response.data
            logging.info(f"Fetched {len(records)} records from '{table_name}' table.")
            
            for record in records:
                well_name = record.get('wlbwellborename')
                if well_name in well_profiles:
                    if data_key in ['wellbore_history', 'lithostratigraphy', 'casing_and_tests', 'drilling_fluid']:
                        well_profiles[well_name][data_key].append(record)
                    else:
                        well_profiles[well_name][data_key] = record
                else:
                    logging.warning(f"Well name '{well_name}' from '{table_name}' not found in initial well list.")
        except Exception as e:
            logging.error(f"Error fetching data from '{table_name}': {e}")
            continue
    
    # Log wells with incomplete profiles
    for well_name, profile in well_profiles.items():
        missing_keys = [key for key in ['lithostratigraphy', 'wellbore_history', 'casing_and_tests', 'drilling_fluid'] 
                        if not profile.get(key)]
        if missing_keys:
            logging.warning(f"Well '{well_name}' is missing data from tables: {', '.join(missing_keys)}")
    
    logging.info("Completed fetching well profiles.")
    return well_profiles
