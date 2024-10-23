# report_delivery.py

import logging
from typing import Dict
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
    logging.info("Stage 5: Report Delivery started.")

    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Since all files are in the same folder, use current_dir as the template path
    template_path = current_dir

    # Setup Jinja2 Environment to load templates from the current directory
    env = Environment(
        loader=FileSystemLoader(searchpath=template_path),
        autoescape=select_autoescape(['html', 'xml'])
    )

    try:
        template = env.get_template("report_template.html")
    except Exception as e:
        logging.error(f"Failed to load HTML template: {e}")
        return

    # Prepare transformed wells data
    transformed_nearby_wells = {}
    for well_name, data in report.get('nearby_wells', {}).items():
        transformed_nearby_wells[well_name] = data

    transformed_similar_wells = {}
    for well_name, data in report.get('similar_wells', {}).items():
        transformed_similar_wells[well_name] = data

    # Render the HTML content using the template
    try:
        rendered_html = template.render(
            title=report.get('title', 'Pre-Well Drilling Report'),
            summary=report.get('summary', ''),
            nearby_wells=transformed_nearby_wells,
            similar_wells=transformed_similar_wells,
            ai_insights=convert_markdown_to_html(report.get('ai_insights', '')),
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

    # Prepare to upload to Supabase Storage
    try:
        supabase = get_supabase_client()
        bucket_name = "reports"
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        pdf_filename = f"pre_well_report_{timestamp}.pdf"

        # Upload PDF directly from memory
        response = supabase.storage.from_(bucket_name).upload(
            pdf_filename,
            pdf,
            file_options={'content-type': 'application/pdf'}
        )

        if hasattr(response, 'status_code') and response.status_code in [200, 201]:
            logging.info(f"PDF report '{pdf_filename}' uploaded to Supabase bucket '{bucket_name}'.")
        elif hasattr(response, 'error') and response.error:
            logging.error(f"Failed to upload PDF report to Supabase: {response.error}")
            return
        else:
            logging.error(f"Failed to upload PDF report to Supabase. Response: {response}")
            return

        # Get the public URL
        public_url_response = supabase.storage.from_(bucket_name).get_public_url(pdf_filename)
        if public_url_response and 'publicURL' in public_url_response:
            public_url = public_url_response['publicURL']
            logging.info(f"Public URL for the report: {public_url}")
        else:
            logging.warning("Failed to retrieve public URL for the report.")

    except Exception as e:
        logging.error(f"Failed to upload PDF report to Supabase: {str(e)}")
        logging.exception("Detailed traceback:")

    logging.info("Stage 5: Report Delivery completed.")
