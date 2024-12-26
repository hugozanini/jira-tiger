from typing import List, Dict, Any
from jira_connection import JiraConnectionManager
import json
import os
from datetime import datetime
import logging

class JiraDataFetcher:
    def __init__(self):
        self.connection = JiraConnectionManager()
        self.logger = logging.getLogger(__name__)
    
    def get_issues(
        self,
        project_id: str,
        labels: List[str],
        start_at: int = 0,
        max_results: int = 50
    ) -> Dict[str, Any]:
        labels_query = " AND ".join([f'labels = "{label}"' for label in labels])
        jql_query = f'project = {project_id} AND {labels_query}'
        
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
            return response.json()
        except Exception as e:
            self.logger.error(f"Error fetching issues: {str(e)}")
            raise
