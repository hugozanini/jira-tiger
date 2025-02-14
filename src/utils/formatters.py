"""Utility functions for formatting Jira data"""

def format_content_to_markdown(content):
    """Convert Jira's JSON content format to markdown"""
    if not content or not isinstance(content, dict):
        return ""

    markdown_text = ""

    for item in content.get("content", []):
        # Handle headings
        if item["type"] == "heading":
            level = item["attrs"]["level"]
            heading_text = "".join(
                content.get("text", "")
                for content in item.get("content", [])
            )
            markdown_text += f"{'#' * level} {heading_text}\n\n"

        # Handle paragraphs
        elif item["type"] == "paragraph":
            paragraph_text = ""
            for content in item.get("content", []):
                if content["type"] == "text":
                    # Check for marks (bold, italic, etc)
                    text = content.get("text", "")
                    if "marks" in content:
                        for mark in content["marks"]:
                            if mark["type"] == "strong":
                                text = f"**{text}**"
                    paragraph_text += text
                elif content["type"] == "mention":
                    paragraph_text += f"@{content['attrs'].get('text', '')}"
                elif content["type"] == "emoji":
                    paragraph_text += f":{content['attrs'].get('shortName', '')[1:-1]}:"
                elif content["type"] == "inlineCard":
                    paragraph_text += f"[{content['attrs'].get('url', '')}]"
            markdown_text += f"{paragraph_text}\n\n"

        # Handle bullet lists
        elif item["type"] == "bulletList":
            for list_item in item.get("content", []):
                list_content = list_item.get("content", [{}])[0].get("content", [])
                item_text = ""
                for content in list_content:
                    if content.get("type") == "text":
                        item_text += content.get("text", "")
                    elif content.get("type") == "mention":
                        item_text += f"@{content['attrs'].get('text', '')}"
                markdown_text += f"* {item_text}\n"
            markdown_text += "\n"

    return markdown_text.strip()