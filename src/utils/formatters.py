"""Utility functions for formatting Jira data"""

def format_content_to_markdown(content):
    """Convert Jira's JSON content format to markdown

    Args:
        content (dict): Jira content in JSON format

    Returns:
        str: Formatted markdown text
    """
    if not content or not isinstance(content, dict):
        return ""

    markdown_text = ""
    for item in content.get("content", []):
        if item["type"] == "paragraph":
            text = "".join(c.get("text", "") for c in item.get("content", []))
            markdown_text += f"{text}\n\n"
    return markdown_text.strip()

def format_custom_field_value(field_value, is_array=False):
    """Safely get custom field values

    Args:
        field_value: The field value to format
        is_array (bool): Whether the field is an array

    Returns:
        str or list: Formatted field value
    """
    if not field_value:
        return None if not is_array else []

    if is_array:
        return [item.get("value") for item in field_value if item and item.get("value")]
    return field_value.get("value")

def format_team_list(teams):
    """Format team values from fields

    Args:
        teams (list): List of team objects

    Returns:
        list: List of team values
    """
    if not teams:
        return []
    return [team.get("value") for team in teams if team and team.get("value")]
