# all_generate_report_functions/report_delivery.py

import logging
from fpdf import FPDF
from typing import Dict
import os
from datetime import datetime
from utils.get_supabase_client import get_supabase_client

class PDFReport(FPDF):
    def header(self):
        # Arial bold 15
        self.set_font('Arial', 'B', 15)
        # Calculate width of title and position
        title = self.title
        w = self.get_string_width(title) + 6
        self.set_x((210 - w) / 2)
        # Colors of frame, background, and text
        self.set_draw_color(0, 0, 0)
        self.set_fill_color(230, 230, 0)
        self.set_text_color(0, 0, 0)
        # Thickness of frame (1 mm)
        self.set_line_width(1)
        # Title
        self.cell(w, 9, title, 1, 1, 'C', 1)
        # Line break
        self.ln(10)

    def footer(self):
        # Position at 1.5 cm from bottom
        self.set_y(-15)
        # Arial italic 8
        self.set_font('Arial', 'I', 8)
        # Text color in gray
        self.set_text_color(128)
        # Page number
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

    def chapter_title(self, label):
        # Arial 12
        self.set_font('Arial', 'B', 12)
        # Background color
        self.set_fill_color(200, 220, 255)
        # Title
        self.cell(0, 10, label, 0, 1, 'L', 1)
        # Line break
        self.ln(2)

    def chapter_body(self, text):
        # Read text file
        self.set_font('Arial', '', 12)
        # Output justified text
        self.multi_cell(0, 10, text)
        # Line break
        self.ln()

    def add_table(self, data, col_widths, headers=None):
        self.set_font('Arial', 'B', 12)
        if headers:
            for i, header in enumerate(headers):
                self.cell(col_widths[i], 10, header, 1, 0, 'C')
            self.ln()
        self.set_font('Arial', '', 12)
        for row in data:
            for i, item in enumerate(row):
                self.cell(col_widths[i], 10, str(item), 1, 0, 'C')
            self.ln()

def deliver_report(report: Dict):
    """
    Generate a PDF report and upload it to Supabase storage.
    Args:
        report (Dict): Compiled report content.
    """
    logging.info("Stage 6: Report Delivery started.")

    # Generate PDF
    pdf = PDFReport()
    pdf.title = report.get('title', 'Report')
    pdf.add_page()

    # Summary
    pdf.chapter_title("Summary")
    pdf.chapter_body(report.get('summary', 'No summary provided.'))

    # Wells Information
    pdf.chapter_title("Nearby Wells")

    for well_name, profile in report.get('wells', {}).items():
        pdf.set_font("Arial", 'B', 12)
        pdf.cell(0, 10, f"Well Name: {well_name}", ln=True)
        pdf.set_font("Arial", '', 12)
        # Create a table for key well information
        headers = ["Field", "Value"]
        data = [
            ["Distance (km)", profile.get('distance_km', 'N/A')],
            ["General Info", profile.get('general_info', {})],
            ["Wellbore History", "\n".join([h.get('content', '') for h in profile.get('wellbore_history', [])])],
            ["Lithostratigraphy", "\n".join([f"{l.get('lithostratigraphic_unit', '')} at {l.get('top_depth_m_md_rkb', '')} m" for l in profile.get('lithostratigraphy', [])])],
            ["Casing and Tests", "\n".join([str(c) for c in profile.get('casing_and_tests', [])])],
            ["Drilling Fluid", "\n".join([str(f) for f in profile.get('drilling_fluid', [])])]
        ]
        # For simplicity, only display key fields; can be customized further
        # Display in key-value pairs
        for field, value in data:
            pdf.set_font("Arial", 'B', 11)
            pdf.cell(40, 10, f"{field}:", 1)
            pdf.set_font("Arial", '', 11)
            # If value is a dict, convert to string
            if isinstance(value, dict):
                value_str = ', '.join([f"{k}: {v}" for k, v in value.items()])
            elif isinstance(value, list):
                value_str = value
                # For better formatting, might need to handle lists differently
                value_str = '\n'.join(map(str, value))
            else:
                value_str = str(value)
            # Wrap text in cell
            pdf.multi_cell(0, 10, value_str, border=1)
        pdf.ln(5)

    # AI Insights
    pdf.chapter_title("AI-Driven Risk Analysis")
    pdf.chapter_body(report.get('ai_insights', 'No insights provided.'))

    # Optionally, add tables comparing raw data of wells
    # For example, comparing casing diameters across wells
    # Assuming multiple wells, but in current case, max 5

    if len(report.get('wells', {})) > 1:
        pdf.chapter_title("Comparison of Well Data")
        # Example table: Well Name, Total Depth, Number of Casing Layers
        headers = ["Well Name", "Total Depth (m)", "Number of Casing Layers"]
        data = []
        for well_name, profile in report.get('wells', {}).items():
            total_depth = profile.get('general_info', {}).get('totalt_maalt_dybde_md_m_rkb', 'N/A')
            num_casings = len(profile.get('casing_and_tests', []))
            data.append([well_name, total_depth, num_casings])
        col_widths = [60, 60, 60]
        pdf.add_table(data, col_widths, headers)

    # Save PDF to a temporary file
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    pdf_filename = f"pre_well_report_{timestamp}.pdf"
    pdf.output(pdf_filename)
    logging.info(f"PDF report '{pdf_filename}' generated successfully.")

    # Upload PDF to Supabase Storage
    try:
        supabase = get_supabase_client()
        
        bucket_name = "reports"
        file_path = pdf_filename  # Placing in the bucket's root

        with open(pdf_filename, 'rb') as file:
            response = supabase.storage.from_(bucket_name).upload(file_path, file, {'content-type': 'application/pdf'})

        # Enhanced error logging
        if response.status_code in [200, 201]:
            logging.info(f"PDF report '{pdf_filename}' uploaded to Supabase bucket '{bucket_name}'.")
        else:
            logging.error(f"Failed to upload PDF report to Supabase. Status code: {response.status_code}")
            logging.error(f"Response: {response.content}")
            return

        # Optionally, get the public URL
        public_url_response = supabase.storage.from_(bucket_name).get_public_url(file_path)
        if public_url_response and 'publicURL' in public_url_response:
            public_url = public_url_response['publicURL']
            logging.info(f"Public URL for the report: {public_url}")
        else:
            logging.warning("Failed to retrieve public URL for the report.")

        # Clean up the local file if desired
        os.remove(pdf_filename)
        logging.info(f"Local PDF file '{pdf_filename}' removed after upload.")

    except Exception as e:
        logging.error(f"Failed to upload PDF report to Supabase: {str(e)}")
        logging.exception("Detailed traceback:")

    logging.info("Stage 6: Report Delivery completed.")
