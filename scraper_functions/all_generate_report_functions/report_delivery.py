# report_delivery.py

import os
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, select_autoescape
from weasyprint import HTML
from utils.get_supabase_client import get_supabase_client
from utils.markdown_to_html import convert_markdown_to_html

def deliver_report(report: dict):
    """
    Generate a PDF report from HTML template and upload it to Supabase storage.
    """
    print("Stage 5: Report Delivery started.")

    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    template_path = current_dir

    # Setup Jinja2 Environment
    env = Environment(
        loader=FileSystemLoader(searchpath=template_path),
        autoescape=select_autoescape(['html', 'xml'])
    )

    try:
        template = env.get_template("report_template.html")
    except Exception as e:
        print(f"Failed to load HTML template: {e}")
        return

    # Prepare transformed wells data
    transformed_nearby_wells = []
    for data in report.get('nearby_wells', []):
        transformed_nearby_wells.append({
            'wlbwellborename': data['wlbwellborename'],
            'distance': data.get('distance', 0),
            'well_profile': convert_markdown_to_html(data['well_profile'])
        })

    transformed_similar_wells = []
    for data in report.get('similar_wells', []):
        transformed_similar_wells.append({
            'wlbwellborename': data['wlbwellborename'],
            'similarity_score': data.get('similarity_score', 0),
            'well_profile': convert_markdown_to_html(data['well_profile'])
        })

    # Extract well names for links
    nearby_well_names = [well['wlbwellborename'] for well in transformed_nearby_wells]
    similar_well_names = [well['wlbwellborename'] for well in transformed_similar_wells]

    # First Render: Render HTML without toc_page_numbers
    try:
        rendered_html = template.render(
            title=report.get('title', 'Pre-Well Drilling Report'),
            summary=report.get('summary', ''),
            nearby_wells=transformed_nearby_wells,
            similar_wells=transformed_similar_wells,
            ai_insights=convert_markdown_to_html(report.get('ai_insights', '')),
            generation_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            nearby_well_names=nearby_well_names,
            similar_well_names=similar_well_names,
            toc_page_numbers={}  # Empty initially
        )
        print("Initial HTML content rendered successfully.")
    except Exception as e:
        print(f"Failed to render HTML template: {e}")
        return

    # Render the HTML to get the page numbers
    try:
        html_obj = HTML(string=rendered_html, base_url=current_dir)
        doc = html_obj.render()

        toc_page_numbers = {}

        # Iterate over pages to map anchors to page numbers
        for page_number, page in enumerate(doc.pages, start=1):
            for anchor in page.anchors:
                # Only map if not already mapped
                if anchor not in toc_page_numbers:
                    toc_page_numbers[anchor] = page_number

        print("Page numbers extracted successfully.")
    except Exception as e:
        print(f"Failed to render HTML for page numbers: {e}")
        return

    # Second Render: Render HTML with toc_page_numbers
    try:
        rendered_html_with_toc = template.render(
            title=report.get('title', 'Pre-Well Drilling Report'),
            summary=report.get('summary', ''),
            nearby_wells=transformed_nearby_wells,
            similar_wells=transformed_similar_wells,
            ai_insights=convert_markdown_to_html(report.get('ai_insights', '')),
            generation_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            nearby_well_names=nearby_well_names,
            similar_well_names=similar_well_names,
            toc_page_numbers=toc_page_numbers
        )
        print("Final HTML content with TOC page numbers rendered successfully.")
    except Exception as e:
        print(f"Failed to render HTML template with page numbers: {e}")
        return

    # Convert HTML to PDF using WeasyPrint
    try:
        final_html = HTML(string=rendered_html_with_toc, base_url=current_dir)
        pdf = final_html.write_pdf()
        print("HTML converted to PDF successfully.")
    except Exception as e:
        print(f"Failed to convert HTML to PDF: {e}")
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
            print(f"PDF report '{pdf_filename}' uploaded to Supabase bucket '{bucket_name}'.")
        elif hasattr(response, 'error') and response.error:
            print(f"Failed to upload PDF report to Supabase: {response.error}")
            return
        else:
            print(f"Failed to upload PDF report to Supabase. Response: {response}")
            return

        # Get the public URL
        public_url_response = supabase.storage.from_(bucket_name).get_public_url(pdf_filename)
        if public_url_response and 'publicURL' in public_url_response:
            public_url = public_url_response['publicURL']
            print(f"Public URL for the report: {public_url}")
        else:
            print("Failed to retrieve public URL for the report.")

    except Exception as e:
        print(f"Failed to upload PDF report to Supabase: {str(e)}")

    print("Stage 5: Report Delivery completed.")
