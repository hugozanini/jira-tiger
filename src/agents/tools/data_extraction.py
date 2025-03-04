from crewai.tools import BaseTool
from pydantic import BaseModel, Field, ConfigDict
from src.connection.jira_connection import JiraConnectionManager

import pandas as pd
import os
from datetime import datetime
from tqdm import tqdm

class JiraDataExtractionSchema(BaseModel):
    """Schema for JiraStorageTools inputs"""
    action: str = Field(description="Action to perform. Possible actions: ingest_board_overview")
    project_id: str = Field(default="", description="ID of the project to extract data from")
    labels: list = Field(default=[], description="Project labels to be filtered")

class JiraDataExtraction(BaseTool):
    name: str = "Tools for extracting data from Jira"
    description: str = "Tools for extracting data from Jira"
    model_config = ConfigDict(arbitrary_types_allowed=True)
    args_schema: type[BaseModel] = JiraDataExtractionSchema
    connection: JiraConnectionManager = Field(default_factory=JiraConnectionManager)
    project_id: str = Field(default="", description="ID of the project to extract data from")
    labels: str = Field(default="", description="Project labels to be filtered")

    def _run(self, action:str, project_id: str, labels: list) -> str:
        """
        Execute data extraction actions
        """
        self.connection = JiraConnectionManager()
        self.project_id = project_id
        self.labels = labels
        actions = {
                "ingest_board_overview": lambda: self.ingest_board_overview(),
            }
        if action not in actions:
                return f"Invalid action. Available actions: {list(actions.keys())}"
        return actions[action]()

    def ingest_board_overview(self) -> str:
        """
        Return the path of a csv file with the board overview including parent and child issues
        """
        parent_issues = self.__get_parent_issues()
        return self.__create_board_overview(parent_issues)

    def __get_issues_count(self):
        """Get the total issues in the project with a given label"""
        jql_query = ' OR '.join(f'labels = "{label}"' for label in self.labels)

        params = {
            "jql": jql_query,
            "startAt": 0,
            "maxResults": 0
        }

        try:
            response = self.connection.make_request(
                method="GET",
                endpoint="/rest/api/3/search",
                params=params
            )
            if not response or response.status_code != 200:
                return None
            return response.json()["total"]
        except Exception:
            return None


    def __get_labeled_issues(self, start_at=0, max_results=50):
        """Get issues with specified labels in a given project using JQL search"""
        jql_query = f'project = {self.project_id}'
        labels_query = " OR ".join([f'labels = "{label}"' for label in self.labels])
        if len(labels_query) > 0:
            jql_query = f'project = {self.project_id} AND ({labels_query})'

        params = {
            "jql": jql_query,
            "startAt": start_at,
            "maxResults": max_results,
            "fields": [ #Remove this in the first execution
                "key",
                "summary",
                "status",
                "labels",
                "created",
                "updated",
                "description",
                "customfield_24910", #Related docs
                "customfield_18717", #Teams
                "customfield_20650", #Product
                "customfield_21943", # Points of contact
                "issuelinks"
            ]
        }

        try:
            response = self.connection.make_request(
                method="GET",
                endpoint="/rest/api/3/search",
                params=params
            )
            if not response or response.status_code != 200:
                return None
            return response.json()
        except Exception:
            return None

    def __get_parent_issues(self, max_results = 50):
        """Get all issues using pagination with the max_results size"""
        all_issues = []
        start_at = 0
        total_issues = self.__get_issues_count()
        if total_issues is None:
            return all_issues

        progress_bar = tqdm(total=total_issues, desc="Fetching issues")

        while True:
            result = self.__get_labeled_issues(start_at)

            if not result or 'issues' not in result:
                break

            issues = result['issues']
            all_issues.extend(issues)

            # Update progress bar with the number of new issues fetched
            progress_bar.update(len(issues))

            if len(issues) < max_results:
                break

            start_at += max_results

        progress_bar.close()
        return all_issues


    def __format_content_to_markdown(self, content):
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


    def __get_linked_issues(self, fields, parent_key):
        """Extract only child issues (both inward and outward) from an issue, excluding parent links"""
        child_issues = []

        for link in fields.get('issuelinks', []):
            # Include outward issues
            if 'outwardIssue' in link:
                child_issues.append(link['outwardIssue']['key'])
            # Include inward issues only if they are not parents
            elif 'inwardIssue' in link and parent_key not in link.get('inwardIssue', {}).get('child_issues', '').split(', '):
                child_issues.append(link['inwardIssue']['key'])

        return child_issues


    def __get_issue_details(self, key):
        """Get details for a specific issue by key"""
        try:
            response = self.connection.make_request(
                method="GET",
                endpoint=f"/rest/api/3/issue/{key}",
                params={
                    "fields": [
                        "key",
                        "summary",
                        "status",
                        "labels",
                        "created",
                        "updated",
                        "description",
                        "customfield_24910",
                        "customfield_18717",
                        "customfield_20650",
                        "customfield_21943",
                        "issuelinks"
                    ]
                }
            )
            if response and response.status_code == 200:
                return response.json()
            return None
        except Exception:
            return None

    def __get_issue_comments(self, issue_key):
        """Get formatted comments for a specific Jira issue"""
        try:
            response = self.connection.make_request(
                method="GET",
                endpoint=f"/rest/api/3/issue/{issue_key}",
                params={"fields": ["comment"]}
            )

            if not response or response.status_code != 200:
                return []

            issue_data = response.json()
            comments = issue_data.get('fields', {}).get('comment', {}).get('comments', [])
            formatted_comments = []

            for comment in comments:
                author = comment.get('author', {}).get('displayName', '')
                updated = comment.get('updated', '')[:10]

                # Process content
                body = comment.get('body', {})
                content = []

                for item in body.get('content', []):
                    if item['type'] == 'paragraph':
                        for text_item in item.get('content', []):
                            if text_item['type'] == 'text':
                                content.append(text_item.get('text', ''))
                            elif text_item['type'] == 'mention':
                                content.append(f"@{text_item['attrs'].get('text', '').replace('@', '')}")

                comment_text = ' '.join(content).strip()
                if not comment_text:
                    comment_text = "No text in the comment, probably the comment content is a media or link"

                formatted_comments.append({
                    'date': updated,
                    'formatted_comment': f"{author} - {comment_text}"
                })

            return formatted_comments

        except Exception:
            return []

    def __create_board_overview(self, parent_issues, base_path="jira-weekly-data"):
        """Create a pandas DataFrame from parent and child issues and save it as CSV"""
        cleaned_issues = []

        # Count total child issues first
        total_child_issues = sum(len(self.__get_linked_issues(parent['fields'], parent['key'])) for parent in parent_issues)
        child_progress = tqdm(total=total_child_issues, desc="Processing issues", position=0, leave=True)
        parent_updates = {}  # Store the most recent update date for each parent

        for parent in parent_issues:
            fields = parent['fields']
            parent_key = parent['key']
            parent_updates[parent_key] = fields.get('updated', '')[:10]

            # Get comments for parent
            parent_comments = self.__get_issue_comments(parent_key)
            all_comments = '\n'.join([f"{c['date']} - {c['formatted_comment']}" for c in parent_comments])
            last_comment = parent_comments[-1]['formatted_comment'] if parent_comments else ''
            last_comment_date = parent_comments[-1]['date'] if parent_comments else ''

            # Get workstream values
            #TODO: Remove this workstream part from the code when sharing the open source version
            workstream_values = [ws.get('value', '') for ws in (fields.get('customfield_20650') or [])]
            if any('WorkstreamA' in value for value in workstream_values):
                workstream = 'Workstream A'
            elif any('MR to scale' in value for value in workstream_values):
                workstream = 'Workstream B'
            else:
                workstream = ', '.join(workstream_values)

            # Parent metadata to be inherited by children
            parent_metadata = {
                'teams': ', '.join([team.get('value', '') for team in (fields.get('customfield_18717') or [])]),
                'workstream': workstream,
                'points_of_contact': ', '.join([poc.get('displayName', '') for poc in (fields.get('customfield_21943') or [])])
            }

            # Add parent issue
            cleaned_issues.append({
                'key': parent_key,
                'summary': fields.get('summary', ''),
                'issue_link': f"https://company.atlassian.net/browse/{parent_key}",
                'status': fields.get('status', {}).get('name', ''),
                'created': fields.get('created', '')[:10],
                'last_update': parent_updates[parent_key],
                'description': self.__format_content_to_markdown(fields.get('description', '')),
                'labels': ', '.join(fields.get('labels', []) or []),
                'related_docs': fields.get('customfield_24910', ''),
                'teams': parent_metadata['teams'],
                'workstream': parent_metadata['workstream'],
                'points_of_contact': parent_metadata['points_of_contact'],
                'child_issues': ', '.join(self.__get_linked_issues(fields, parent_key)),
                'parent_issue': '',
                'comments': all_comments,
                'last_comment_date': last_comment_date,
                'last_comment': last_comment
            })

            # Process child issues
            for link in fields.get('issuelinks', []):
                child_key = None
                if 'outwardIssue' in link:
                    child_key = link['outwardIssue']['key']
                elif 'inwardIssue' in link:
                    child_key = link['inwardIssue']['key']

                if child_key:
                    child = self.__get_issue_details(child_key)
                    if child:
                        child_fields = child['fields']
                        child_update = child_fields.get('updated', '')[:10]

                        # Get comments for child
                        child_comments = self.__get_issue_comments(child_key)
                        child_all_comments = '\n'.join([f"{c['date']} - {c['formatted_comment']}" for c in child_comments])
                        child_last_comment = child_comments[-1]['formatted_comment'] if child_comments else ''
                        child_last_comment_date = child_comments[-1]['date'] if child_comments else ''

                        # Update parent's last_update if child is more recent
                        if child_update > parent_updates[parent_key]:
                            parent_updates[parent_key] = child_update

                        cleaned_issues.append({
                            'key': child['key'],
                            'summary': child_fields.get('summary', ''),
                            'issue_link': f"https://company.atlassian.net/browse/{child['key']}",
                            'status': child_fields.get('status', {}).get('name', ''),
                            'created': child_fields.get('created', '')[:10],
                            'last_update': child_update,
                            'description': self.__format_content_to_markdown(child_fields.get('description', '')),
                            'labels': ', '.join(child_fields.get('labels', []) or []),
                            'related_docs': child_fields.get('customfield_24910', ''),
                            'teams': parent_metadata['teams'],
                            'workstream': parent_metadata['workstream'],
                            'points_of_contact': parent_metadata['points_of_contact'],
                            'child_issues': '',
                            'parent_issue': parent_key,
                            'comments': child_all_comments,
                            'last_comment_date': child_last_comment_date,
                            'last_comment': child_last_comment
                        })
                    child_progress.update(1)

        child_progress.close()

        # Create DataFrame
        df = pd.DataFrame(cleaned_issues)

        # Generate filename with current date
        current_date = datetime.now().strftime('%Y-%m-%d')
        filename = f"mr-program-board-overview-{current_date}.csv"

        # Create full path and ensure directory exists
        full_path = os.path.join(base_path, filename)
        os.makedirs(base_path, exist_ok=True)

        # Save DataFrame
        df.to_csv(full_path, index=False)
        return "Project overview saved at " + base_path + "/" + filename

