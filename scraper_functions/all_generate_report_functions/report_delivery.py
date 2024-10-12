# all_generate_report_functions/report_delivery.py

import logging
from typing import Dict  # Added import
import os
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, select_autoescape
from weasyprint import HTML
from utils.get_supabase_client import get_supabase_client
from utils.markdown_to_html import convert_markdown_to_html

def deliver_report(report: Dict):
    """
    Generate a PDF report from HTML template and upload it to Supabase storage.
    Args:
        report (Dict): Compiled report content.
    """
    logging.info("Stage 6: Report Delivery started.")

    # Setup Jinja2 Environment
    env = Environment(
        loader=FileSystemLoader(searchpath="templates"),
        autoescape=select_autoescape(['html', 'xml'])
    )

    try:
        template = env.get_template("report_template.html")
    except Exception as e:
        logging.error(f"Failed to load HTML template: {e}")
        return

    # Convert AI-driven insights from markdown to HTML
    ai_insights_html = convert_markdown_to_html(report.get('ai_insights', 'No insights provided.'))

    # Prepare transformed wells data (as done previously)
    transformed_wells = {}
    for well_name, profile in report.get('wells', {}).items():
        transformed_profile = {
            'distance_km': profile.get('distance_km', 'N/A'),
            'general_info': [],
            'wellbore_history': [],
            'lithostratigraphy': [],
            'casing_and_tests': [],
            'drilling_fluid': []
        }

        # General Info
        general_info = profile.get('general_info') or {}
        for key, value in general_info.items():
            transformed_profile['general_info'].append(f"{key}: {value}")

        # Wellbore History
        wellbore_history = profile.get('wellbore_history', [])
        for history in wellbore_history:
            content = history.get('content', 'N/A').replace('\n', ' ')
            transformed_profile['wellbore_history'].append(content)

        # Lithostratigraphy
        lithostratigraphy = profile.get('lithostratigraphy', [])
        for litho in lithostratigraphy:
            unit = litho.get('lithostratigraphic_unit', 'N/A')
            depth = litho.get('top_depth_m_md_rkb', 'N/A')
            transformed_profile['lithostratigraphy'].append(f"{unit} at {depth} m")

        # Casing and Tests
        casing_and_tests = profile.get('casing_and_tests', [])
        for casing in casing_and_tests:
            # Format casing information as needed
            casing_info = ", ".join([f"{k}: {v}" for k, v in casing.items() if k != 'wlbwellborename'])
            transformed_profile['casing_and_tests'].append(casing_info)

        # Drilling Fluid
        drilling_fluid = profile.get('drilling_fluid', [])
        for fluid in drilling_fluid:
            # Format drilling fluid information as needed
            fluid_info = ", ".join([f"{k}: {v}" for k, v in fluid.items() if k != 'wlbwellborename'])
            transformed_profile['drilling_fluid'].append(fluid_info)

        transformed_wells[well_name] = transformed_profile

    # Render the HTML content using the template
    try:
        rendered_html = template.render(
            title=report.get('title', 'Pre-Well Drilling Report'),
            summary=report.get('summary', 'This report provides a comprehensive analysis of the proposed drilling site, including nearby wells and AI-driven risk assessments.'),
            wells=transformed_wells,
            ai_insights=ai_insights_html,  # Pass HTML version of the AI insights
            generation_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        logging.info("HTML content rendered successfully.")
    except Exception as e:
        logging.error(f"Failed to render HTML template: {e}")
        return

    # Convert HTML to PDF using WeasyPrint
    try:
        pdf = HTML(string=rendered_html).write_pdf()
        logging.info("HTML converted to PDF successfully.")
    except Exception as e:
        logging.error(f"Failed to convert HTML to PDF: {e}")
        return

    # Save PDF to a temporary file
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    pdf_filename = f"pre_well_report_{timestamp}.pdf"
    try:
        with open(pdf_filename, "wb") as f:
            f.write(pdf)
        logging.info(f"PDF report '{pdf_filename}' saved locally.")
    except Exception as e:
        logging.error(f"Failed to save PDF file: {e}")
        return

    # Upload PDF to Supabase Storage
    try:
        supabase = get_supabase_client()
        bucket_name = "reports"
        file_path = pdf_filename

        with open(pdf_filename, 'rb') as file:
            response = supabase.storage.from_(bucket_name).upload(file_path, file, {'content-type': 'application/pdf'})

        # Enhanced error logging
        if hasattr(response, 'status_code') and response.status_code in [200, 201]:
            logging.info(f"PDF report '{pdf_filename}' uploaded to Supabase bucket '{bucket_name}'.")
        elif hasattr(response, 'error') and response.error:
            logging.error(f"Failed to upload PDF report to Supabase: {response.error}")
            return
        else:
            logging.error(f"Failed to upload PDF report to Supabase. Response: {response}")
            return

        # Optionally, get the public URL
        public_url_response = supabase.storage.from_(bucket_name).get_public_url(file_path)
        if public_url_response and 'publicURL' in public_url_response:
            public_url = public_url_response['publicURL']
            logging.info(f"Public URL for the report: {public_url}")
        else:
            logging.warning("Failed to retrieve public URL for the report.")

        # Clean up the local file
        os.remove(pdf_filename)
        logging.info(f"Local PDF file '{pdf_filename}' removed after upload.")

    except Exception as e:
        logging.error(f"Failed to upload PDF report to Supabase: {str(e)}")
        logging.exception("Detailed traceback:")

    logging.info("Stage 6: Report Delivery completed.")
