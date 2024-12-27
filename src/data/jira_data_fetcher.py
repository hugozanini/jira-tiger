import os
from datetime import datetime
import pandas as pd
import json
import logging
from src.connection.jira_connection import JiraConnectionManager
from src.utils.formatters import format_content_to_markdown
class JiraDataFetcher:
    def __init__(self):
        self.connection = JiraConnectionManager()
        self.current_week = None
        self.labels = []
        self.logger = logging.getLogger(__name__)

    def __del__(self):
        """Ensure proper cleanup of resources"""
        if hasattr(self, 'connection') and hasattr(self.connection, 'session'):
            self.connection.session.close()

    def get_labeled_issues(self, project, labels, start_at=0, max_results=50):
        """Get issues with specified labels in a given project using JQL search"""
        labels_query = " AND ".join([f'labels = "{label}"' for label in labels])
        jql_query = f'project = {project} AND {labels_query}'

        params = {
            "jql": jql_query,
            "startAt": start_at,
            "maxResults": max_results,
            "fields": [
                "key",
                "summary",
                "status",
                "labels",
                "assignee",
                "created",
                "updated"
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

    def get_all_labeled_issues(self, project, labels):
        """Get all issues using pagination"""
        all_issues = []
        start_at = 0
        max_results = 50

        while True:
            result = self.get_labeled_issues(project, labels, start_at, max_results)

            if not result or 'issues' not in result:
                break

            issues = result['issues']
            all_issues.extend(issues)

            if len(issues) < max_results:
                break

            start_at += max_results

        return all_issues

    def get_linked_issues(self, issue_key):
        """Get all linked issues that are children of the given issue"""
        if not isinstance(issue_key, str):
            return []

        API_ISSUE_ENDPOINT = f"/rest/api/3/issue/{issue_key}"
        try:
            response = self.connection.make_request(
                method="GET",
                endpoint=API_ISSUE_ENDPOINT,
                params={"fields": "issuelinks"}
            )
            if not response or response.status_code != 200:
                return []

            data = response.json()
            links = data.get("fields", {}).get("issuelinks", [])
            return [{
                "key": link.get("outwardIssue", {}).get("key"),
                "type": link.get("type", {}).get("name")
            } for link in links if "outwardIssue" in link]
        except Exception:
            return []

    def get_child_issues(self, issue_key):
        """Get child issues using JQL search"""
        if not issue_key:
            return []

        try:
            jql = f'parent = {issue_key} OR "Epic Link" = {issue_key}'
            response = self.connection.make_request(
                method="GET",
                endpoint="/rest/api/3/search",
                params={
                    "jql": jql,
                    "fields": "summary,status,assignee,key,issuetype"
                }
            )

            # Match notebook's error handling
            if not response or response.status_code != 200:
                return []

            data = response.json()
            return [{
                "key": issue.get("key"),
                "summary": issue["fields"].get("summary"),
                "status": issue["fields"].get("status", {}).get("name"),
                "assignee": issue["fields"].get("assignee", {}).get("displayName") if issue["fields"].get("assignee") else None,
                "issuetype": issue["fields"].get("issuetype", {}).get("name")
            } for issue in data.get("issues", [])]
        except Exception:
            return []

    def get_clean_issue_info(self, issue_key):
        """Get detailed information for a specific issue"""
        if not issue_key:
            return None

        API_ISSUE_ENDPOINT = f"/rest/api/3/issue/{issue_key}"
        params = {
            "fields": "*all",
            "expand": "changelog,renderedFields"
        }

        try:
            response = self.connection.make_request(
                method="GET",
                endpoint=API_ISSUE_ENDPOINT,
                params=params
            )

            if not response or response.status_code != 200:
                return None

            data = response.json()
            fields = data.get("fields", {})

            # Get linked and child issues
            linked_issues = []
            child_issues = []

            if fields:
                linked_issues = self.get_linked_issues(issue_key)
                child_issues = self.get_child_issues(issue_key)

            labels = fields.get("labels", [])

            # Safely get custom field values
            launch_phase = fields.get("customfield_25484")
            launch_phase_value = launch_phase.get("value") if launch_phase else None

            product = fields.get("customfield_20650", [{}])
            product_value = product[0].get("value") if product and product[0] else None

            business_area = fields.get("customfield_18711")
            business_area_value = business_area.get("value") if business_area else None

            # Safely get teams with default empty list
            teams = fields.get("customfield_18717", [])
            team_values = [team.get("value") for team in teams if team and team.get("value")]

            # Safely get points of contact with default empty list
            contacts = fields.get("customfield_21943", [])
            contact_list = [
                contact.get("displayName")
                for contact in contacts
                if contact and contact.get("displayName")
            ]

            clean_data = {
                "basic_info": {
                    "key": issue_key,
                    "summary": fields.get("summary"),
                    "status": fields.get("status", {}).get("name"),
                    "assignee": fields.get("assignee", {}).get("displayName") if fields.get("assignee") else None,
                    "description": format_content_to_markdown(fields.get("description", {}))
                },
                "project_details": {
                    "project_target": fields.get("customfield_18723"),
                    "project_start": fields.get("customfield_18722"),
                    "launch_phase": launch_phase_value,
                    "product": product_value,
                    "public_description": fields.get("customfield_18718")
                },
                "team_info": {
                    "teams": team_values,
                    "data_business_area": business_area_value,
                    "points_of_contact": contact_list
                },
                "labels": labels,
                "comments": [{
                    "author": comment.get("author", {}).get("displayName"),
                    "created": comment.get("created"),
                    "body": format_content_to_markdown(comment.get("body", {}))
                } for comment in fields.get("comment", {}).get("comments", [])]
            }

            # Match notebook's logic for linked/child issues
            if "roadmap-mr-program-2025" in labels and linked_issues and not child_issues:
                clean_data["child_issues"] = linked_issues
            else:
                clean_data["linked_issues"] = linked_issues
                if child_issues:
                    clean_data["child_issues"] = child_issues

            return json.dumps(clean_data, indent=2)

        except Exception as e:
            self.logger.error(f"Error processing issue {issue_key}: {e}")
            return None


    def get_all_related_issues(self, issue_key, parent_key=None, processed_keys=None):
        """Recursively get all related issues"""
        if processed_keys is None:
            processed_keys = set()

        if not issue_key or issue_key in processed_keys:
            return []

        processed_keys.add(issue_key)
        all_issues = []

        # Get issue details
        try:
            issue_info = self.get_clean_issue_info(issue_key)
            if not issue_info:
                return []

            issue_data = json.loads(issue_info)
            issue_data["parent_issue"] = parent_key
            issue_data["extraction_date"] = datetime.now().isoformat()
            all_issues.append(issue_data)

            # Process child issues
            children = self.get_child_issues(issue_key)
            for child in children:
                if child and isinstance(child, dict) and "key" in child:
                    child_issues = self.get_all_related_issues(child["key"], issue_key, processed_keys)
                    all_issues.extend(child_issues)

            # Process linked issues for roadmap items
            if any(label in self.labels for label in issue_data.get("labels", [])):
                linked = self.get_linked_issues(issue_key)
                for link in linked:
                    if link and "key" in link:
                        linked_issues = self.get_all_related_issues(link["key"], issue_key, processed_keys)
                        all_issues.extend(linked_issues)

            return all_issues
        except Exception:
            return []


    def get_issues_batch(self, issue_keys):
        """Fetch multiple issues in a single request"""
        if not issue_keys:
            return []

        jql = f'key in ({",".join(issue_keys)})'
        return self.connection.make_request(
            method="GET",
            endpoint="/rest/api/3/search",
            params={"jql": jql}
        ).json()

    def get_weekly_jira_data(self, project_id, labels, limit=None):
        """
        Get all issues and their data recursively

        Args:
            project_id: The Jira project ID
            labels: List of labels to filter issues
            limit: Maximum number of parent issues to process (None for all issues)
        """
        self.labels = labels
        all_issues = []
        processed_keys = set()

        # Get all parent issues
        parent_issues = self.get_all_labeled_issues(project_id, labels)

        # Apply limit if specified
        if limit:
            parent_issues = parent_issues[:limit]

        for parent in parent_issues:
            parent_issues = self.get_all_related_issues(parent["key"], None, processed_keys)
            all_issues.extend(parent_issues)

        return all_issues


    def save_weekly_data(self, issues_data):
        """Save the weekly data with specific columns"""
        week_number = datetime.now().strftime("%Y-W%W")
        folder_path = f"jira_weekly_data/{week_number}"
        os.makedirs(folder_path, exist_ok=True)

        # Save raw JSON data
        with open(f"{folder_path}/raw_data.json", "w") as f:
            json.dump(issues_data, f, indent=2)

        # Extract and format the data for DataFrame
        formatted_data = []
        for issue in issues_data:
            formatted_issue = self._format_issue_for_df(issue)
            formatted_data.append(formatted_issue)

        # Convert to DataFrame and save
        df = pd.DataFrame(formatted_data)
        df.to_csv(f"{folder_path}/issues_data.csv", index=False)

        return week_number

    def _format_issue_for_df(self, issue):
        """Format issue data for DataFrame"""
        basic_info = issue.get("basic_info", {})
        project_details = issue.get("project_details", {})
        team_info = issue.get("team_info", {})

        return {
            "key": basic_info.get("key"),
            "summary": basic_info.get("summary"),
            "status": basic_info.get("status"),
            "assignee": basic_info.get("assignee"),
            "points_of_contact": ", ".join(team_info.get("points_of_contact", [])),
            "project_target": project_details.get("project_target"),
            "project_start": project_details.get("project_start"),
            "launch_phase": project_details.get("launch_phase"),
            "product": project_details.get("product"),
            "description": basic_info.get("description"),
            "public_description": project_details.get("public_description"),
            "teams": ", ".join(team_info.get("teams", [])),
            "business_area": team_info.get("business_area"),
            "comments": issue.get("comments"),
            "linked_issues": issue.get("linked_issues"),
            "parent_issue": issue.get("parent_issue"),
            "extraction_date": issue.get("extraction_date"),
            "child_issues": issue.get("child_issues")
        }

    def _get_custom_field_value(self, fields, field_name, is_array=False):
        """Safely get custom field values"""
        field_value = fields.get(field_name)
        if not field_value:
            return None if not is_array else []

        if is_array:
            return [item.get("value") for item in field_value if item and item.get("value")]
        return field_value.get("value")

    def _get_contacts(self, fields):
        """Get contact list from fields with proper None handling"""
        contacts = fields.get("customfield_21943")
        if not contacts:  # Handle None or empty value
            return []
        return [contact.get("displayName") for contact in contacts if contact and contact.get("displayName")]

    def _get_comments(self, fields):
        """Get formatted comments from fields"""
        comments = fields.get("comment", {}).get("comments", [])
        return [{
            "author": comment.get("author", {}).get("displayName"),
            "created": comment.get("created"),
            "body": format_content_to_markdown(comment.get("body", {}))
        } for comment in comments]

    def _get_teams(self, fields):
        """Get team values from fields"""
        teams = fields.get("customfield_18717", [])
        return [team.get("value") for team in teams if team and team.get("value")]