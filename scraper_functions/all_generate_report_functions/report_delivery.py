# all_generate_report_functions/report_delivery.py

import logging
from fpdf import FPDF
from typing import Dict
import os
from datetime import datetime
from utils.get_supabase_client import get_supabase_client  # Corrected import

def deliver_report(report: Dict):
    """
    Generate a PDF report and upload it to Supabase storage.
    Args:
        report (Dict): Compiled report content.
    """
    logging.info("Stage 6: Report Delivery started.")

    # Generate PDF
    pdf = FPDF()
    pdf.add_page()

    # Title
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(0, 10, report['title'], ln=True, align='C')

    # Summary
    pdf.set_font("Arial", 'B', 12)
    pdf.cell(0, 10, "Summary", ln=True)
    pdf.set_font("Arial", '', 12)
    pdf.multi_cell(0, 10, report['summary'])

    # Wells Information
    pdf.set_font("Arial", 'B', 12)
    pdf.cell(0, 10, "Nearby Wells", ln=True)
    pdf.set_font("Arial", '', 12)
    for well_name, profile in report['wells'].items():
        pdf.cell(0, 10, f"Well Name: {well_name}", ln=True)
        pdf.cell(0, 10, f"Distance: {profile.get('distance_km', 'N/A')} km", ln=True)
        pdf.cell(0, 10, f"General Info: {profile.get('general_info', 'N/A')}", ln=True)
        pdf.multi_cell(0, 10, f"Wellbore History: {profile.get('wellbore_history', [])}")
        pdf.multi_cell(0, 10, f"Lithostratigraphy: {profile.get('lithostratigraphy', [])}")
        pdf.multi_cell(0, 10, f"Casing and Tests: {profile.get('casing_and_tests', [])}")
        pdf.multi_cell(0, 10, f"Drilling Fluid: {profile.get('drilling_fluid', [])}")
        pdf.cell(0, 10, "-"*50, ln=True)

    # AI Insights
    pdf.set_font("Arial", 'B', 12)
    pdf.cell(0, 10, "AI-Driven Risk Analysis", ln=True)
    pdf.set_font("Arial", '', 12)
    pdf.multi_cell(0, 10, report['ai_insights'])

    # Save PDF to a temporary file
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    pdf_filename = f"pre_well_report_{timestamp}.pdf"
    pdf.output(pdf_filename)
    logging.info(f"PDF report '{pdf_filename}' generated successfully.")

    # Upload PDF to Supabase Storage
    try:
        supabase = get_supabase_client()
        bucket_name = os.getenv('SUPABASE_BUCKET_NAME', 'reports')  # Ensure you have a 'reports' bucket
        with open(pdf_filename, 'rb') as file:
            supabase.storage.from_(bucket_name).upload(f"reports/{pdf_filename}", file, {'content-type': 'application/pdf'})
        logging.info(f"PDF report '{pdf_filename}' uploaded to Supabase bucket '{bucket_name}'.")

        # Optionally, get the public URL
        public_url = supabase.storage.from_(bucket_name).get_public_url(f"reports/{pdf_filename}").publicURL
        logging.info(f"Public URL for the report: {public_url}")

        # Clean up the local file if desired
        os.remove(pdf_filename)
        logging.info(f"Local PDF file '{pdf_filename}' removed after upload.")

    except Exception as e:
        logging.error(f"Failed to upload PDF report to Supabase: {e}")

    logging.info("Stage 6: Report Delivery completed.")
