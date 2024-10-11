# all_generate_report_functions/report_delivery.py

import logging
from fpdf import FPDF
from typing import Dict
import os
from datetime import datetime
from utils.get_supabase_client import get_supabase_client

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
    pdf.cell(0, 10, report.get('title', 'Report'), ln=True, align='C')

    # Summary
    pdf.set_font("Arial", 'B', 12)
    pdf.cell(0, 10, "Summary", ln=True)
    pdf.set_font("Arial", '', 12)
    pdf.multi_cell(0, 10, report.get('summary', 'No summary provided.'))

    # Wells Information
    pdf.set_font("Arial", 'B', 12)
    pdf.cell(0, 10, "Nearby Wells", ln=True)
    pdf.set_font("Arial", '', 12)
    for well_name, profile in report.get('wells', {}).items():
        pdf.cell(0, 10, f"Well Name: {well_name}", ln=True)
        pdf.cell(0, 10, f"Distance: {profile.get('distance_km', 'N/A')} km", ln=True)
        pdf.cell(0, 10, f"General Info: {profile.get('general_info', 'N/A')}", ln=True)
        pdf.multi_cell(0, 10, f"Wellbore History: {profile.get('wellbore_history', 'N/A')}")
        pdf.multi_cell(0, 10, f"Lithostratigraphy: {profile.get('lithostratigraphy', 'N/A')}")
        pdf.multi_cell(0, 10, f"Casing and Tests: {profile.get('casing_and_tests', 'N/A')}")
        pdf.multi_cell(0, 10, f"Drilling Fluid: {profile.get('drilling_fluid', 'N/A')}")
        pdf.cell(0, 10, "-"*50, ln=True)

    # AI Insights
    pdf.set_font("Arial", 'B', 12)
    pdf.cell(0, 10, "AI-Driven Risk Analysis", ln=True)
    pdf.set_font("Arial", '', 12)
    pdf.multi_cell(0, 10, report.get('ai_insights', 'No insights provided.'))

    # Save PDF to a temporary file
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    pdf_filename = f"pre_well_report_{timestamp}.pdf"
    pdf.output(pdf_filename)
    logging.info(f"PDF report '{pdf_filename}' generated successfully.")

    # Upload PDF to Supabase Storage
    try:
        supabase = get_supabase_client()
        
        # Check if the client is authenticated
        try:
            user = supabase.auth.get_user()
            if user is None or user.user is None:
                logging.error("Supabase client is not authenticated.")
                logging.info("Attempting to get session...")
                session = supabase.auth.get_session()
                if session:
                    logging.info(f"Session found. Access token: {session.access_token[:10]}...")
                else:
                    logging.error("No session found.")
                return
            logging.info(f"Authenticated as user: {user.user.email}")
        except Exception as auth_error:
            logging.error(f"Error during authentication check: {str(auth_error)}")
            logging.exception("Authentication error traceback:")
            return

        bucket_name = "reports"
        file_path = pdf_filename  # Placing in the bucket's root

        with open(pdf_filename, 'rb') as file:
            response = supabase.storage.from_(bucket_name).upload(file_path, file, {'content-type': 'application/pdf'})

        # Enhanced error logging
        if hasattr(response, 'status_code') and response.status_code in [200, 201]:
            logging.info(f"PDF report '{pdf_filename}' uploaded to Supabase bucket '{bucket_name}'.")
        else:
            logging.error(f"Failed to upload PDF report to Supabase.")
            if hasattr(response, 'status_code'):
                logging.error(f"Status code: {response.status_code}")
            if hasattr(response, 'content'):
                logging.error(f"Response content: {response.content}")
            if hasattr(response, 'text'):
                logging.error(f"Response text: {response.text}")
            return

        # ... [rest of the function remains unchanged]

    except Exception as e:
        logging.error(f"Failed to upload PDF report to Supabase: {str(e)}")
        logging.exception("Detailed traceback:")

    logging.info("Stage 6: Report Delivery completed.")