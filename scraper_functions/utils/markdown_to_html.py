import markdown
import logging

def convert_markdown_to_html(markdown_content: str) -> str:
    """
    Convert Markdown text to HTML.
    Args:
        markdown_content (str): The markdown content to convert.
    Returns:
        str: The converted HTML content.
    """
    try:
        html_content = markdown.markdown(markdown_content)
        logging.info("Markdown successfully converted to HTML.")
        return html_content
    except Exception as e:
        logging.error(f"Failed to convert markdown to HTML: {e}")
        return "<p>Failed to convert insights to HTML format.</p>"