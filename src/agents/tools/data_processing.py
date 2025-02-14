from crewai.tools import BaseTool
from pydantic import BaseModel, Field, ConfigDict

import pandas as pd
import os

class JiraDataProcessingSchema(BaseModel):
    """Schema for JiraStorageTools inputs"""
    action: str = Field(description="Action to perform. Possible actions: create_teams_markdowns")
    csv_file: str = Field(default=None, description="Path with the board csv file")
    start_date: str = Field(default=None, description="Start date to be filtered")
    end_date: str = Field(default=None, description="End date to be filtered")

class JiraDataProcessing(BaseTool):
    name: str = "JiraDataProcessing"
    description: str = "Tools for processing jira data"
    model_config = ConfigDict(arbitrary_types_allowed=True)
    args_schema: type[BaseModel] = JiraDataProcessingSchema
    csv_file: str = Field(default=None, description="Path with the board csv file")
    start_date: str = Field(default=None, description="Start date to be filtered")
    end_date: str = Field(default=None, description="End date to be filtered")

    def _run(self, action:str, csv_file:str, start_date: str, end_date: list) -> str:
        """
        Execute data extraction actions
        """
        self.csv_file = csv_file
        self.start_date = start_date
        self.end_date = end_date
        actions = {
            "create_teams_markdowns": lambda: self.generate_teams_markdown(),
        }
        if action not in actions:
                return f"Invalid action. Available actions: {list(actions.keys())}"
        return actions[action]()

    def generate_teams_markdown(self, base_path = "teams-markdown", separate_by_team=True):
        # Read CSV file
        df = pd.read_csv(self.csv_file)

        # Convert dates to datetime
        df['last_update'] = pd.to_datetime(df['last_update'])
        start_date = pd.to_datetime(self.start_date)
        end_date = pd.to_datetime(self.end_date)

        # Process teams column (take first team when multiple are present)
        df['primary_team'] = df['teams'].apply(lambda x: x.split(',')[0].strip() if pd.notna(x) else 'Unassigned')
        # Create dictionary to store parent-child relationships
        parent_child = {}
        for _, row in df.iterrows():
            if pd.notna(row['parent_issue']):
                if row['parent_issue'] not in parent_child:
                    parent_child[row['parent_issue']] = []
                parent_child[row['parent_issue']].append(row)

        # Sort parents by number of children
        parent_counts = {k: len(v) for k, v in parent_child.items()}

        def get_parent_priority(issue_key):
            return parent_counts.get(issue_key, 0)

        if separate_by_team:
            # Generate separate markdown files for each team
            teams = sorted(df['primary_team'].unique())
            for team in teams:
                team_issues = df[df['primary_team'] == team]

                # Generate markdown content for the team
                markdown = f"# Team name: {team}\n\n"
                markdown += "All the issues below are from the same team.\n\n"

                # Add team's points of contact
                team_contacts = set()
                for contacts in team_issues['points_of_contact'].dropna():
                    team_contacts.update([c.strip() for c in contacts.split(',')])

                markdown += "## Points of Contact\n"
                for contact in sorted(team_contacts):
                    markdown += f"- {contact}\n"
                markdown += "\n"

                # Updated Issues
                markdown += "## Updated Issues\n\n"
                updated = team_issues[
                    (team_issues['last_update'] >= self.start_date) &
                    (team_issues['last_update'] <= self.end_date)
                ]

                if not updated.empty:
                    sorted_issues = sorted(
                        updated.iterrows(),
                        key=lambda x: get_parent_priority(x[1]['key']),
                        reverse=True
                    )

                    for _, issue in sorted_issues:
                        if pd.isna(issue['parent_issue']):  # Only process parent issues first
                            markdown += self.__format_issue(issue)
                            if issue['key'] in parent_child:
                                for child in parent_child[issue['key']]:
                                    markdown += self.__format_issue(child, is_child=True)
                else:
                    markdown += "No issues updated during this period.\n\n"

                # Not Updated Issues
                markdown += "## Not Updated Issues\n\n"
                not_updated = team_issues[
                    (team_issues['last_update'] < self.start_date) |
                    (team_issues['last_update'] > self.end_date)
                ]

                if not not_updated.empty:
                    sorted_issues = sorted(
                        not_updated.iterrows(),
                        key=lambda x: get_parent_priority(x[1]['key']),
                        reverse=True
                    )

                    for _, issue in sorted_issues:
                        if pd.isna(issue['parent_issue']):  # Only process parent issues first
                            markdown += self.__format_issue(issue)
                            if issue['key'] in parent_child:
                                for child in parent_child[issue['key']]:
                                    markdown += self.__format_issue(child, is_child=True)
                else:
                    markdown += "No issues outside this period.\n\n"

                markdown += f"## This ends all the issues from the team {team}\n\n"

                # Create date-based subfolder
                date_folder = f"{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}"
                full_path = os.path.join(base_path, date_folder)
                os.makedirs(full_path, exist_ok=True)

                # Save to file
                file_path = f"{full_path}/{team.replace(' ', '_')}.md"
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(markdown)
            return "Markdowns saved at " + full_path
        else:
            #TODO: Remove this workstream part from the code when sharing the open source version
            # Generate a single markdown file separating issues by workstream
            workstreams = ['Multi-repos Incremental', 'Multi-repos to scale']
            markdown = "# Multi-repos Program Status Report\n\n"

            for workstream in workstreams:
                workstream_issues = df[df['workstream'] == workstream]

                if not workstream_issues.empty:
                    markdown += f"## Workstream: {workstream}\n\n"

                    # Get unique teams in this workstream
                    teams = sorted(workstream_issues['primary_team'].unique())

                    for team in teams:
                        team_issues = workstream_issues[workstream_issues['primary_team'] == team]

                        # Get points of contact for the team
                        team_contacts = set()
                        for contacts in team_issues['points_of_contact'].dropna():
                            team_contacts.update([c.strip() for c in contacts.split(',')])

                        markdown += f"### Team name: {team}\n\n"
                        markdown += "#### Points of Contact\n"
                        for contact in sorted(team_contacts):
                            markdown += f"- {contact}\n"
                        markdown += "\n"

                        # Updated Issues
                        markdown += "#### Updated Issues\n\n"
                        updated = team_issues[
                            (team_issues['last_update'] >= self.start_date) &
                            (team_issues['last_update'] <= self.end_date)
                        ]

                        if not updated.empty:
                            sorted_issues = sorted(
                                updated.iterrows(),
                                key=lambda x: get_parent_priority(x[1]['key']),
                                reverse=True
                            )

                            for _, issue in sorted_issues:
                                if pd.isna(issue['parent_issue']):
                                    markdown += self.__format_issue(issue)
                                    if issue['key'] in parent_child:
                                        for child in parent_child[issue['key']]:
                                            markdown += self.__format_issue(child, is_child=True)
                        else:
                            markdown += "No issues updated during this period.\n\n"

                        # Not Updated Issues
                        markdown += "#### Not Updated Issues\n\n"
                        not_updated = team_issues[
                            (team_issues['last_update'] < self.start_date) |
                            (team_issues['last_update'] > self.end_date)
                        ]

                        if not not_updated.empty:
                            sorted_issues = sorted(
                                not_updated.iterrows(),
                                key=lambda x: get_parent_priority(x[1]['key']),
                                reverse=True
                            )

                            for _, issue in sorted_issues:
                                if pd.isna(issue['parent_issue']):
                                    markdown += self.__format_issue(issue)
                                    if issue['key'] in parent_child:
                                        for child in parent_child[issue['key']]:
                                            markdown += self.__format_issue(child, is_child=True)
                        else:
                            markdown += "No issues outside this period.\n\n"

                        markdown += f"#### This ends all the issues from the team {team}\n\n"

            # Create date-based subfolder
            date_folder = f"{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}"
            full_path = os.path.join(base_path, date_folder)
            os.makedirs(full_path, exist_ok=True)

            # Save to file
            file_path = f"{full_path}/workstream_report.md"
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(markdown)
            return "Markdown saved at " + full_path

    def __format_issue(self, issue, is_child=False):
        prefix = "  " if is_child else ""
        markdown = f"{prefix}- **{issue['key']}** - {issue['summary']}\n\n"

        # Add description as quote if present
        if pd.notna(issue['description']):
            markdown += f"{prefix}  **Description:**\n"
            description_lines = issue['description'].split('\n')
            markdown += f"{prefix}  > " + f"\n{prefix}  > ".join(description_lines) + "\n\n"

        # Add comments history as quotes if present
        if pd.notna(issue['comments']):
            markdown += f"{prefix}  **Comments History:**\n"
            comment_lines = issue['comments'].split('\n')
            for comment in comment_lines:
                if comment.strip():
                    markdown += f"{prefix}  > {comment}\n"
            markdown += "\n"

        # Add last comment if present
        if pd.notna(issue['last_comment']):
            markdown += f"{prefix}  **Last Comment ({issue['last_comment_date']}):**\n"
            markdown += f"{prefix}  > {issue['last_comment']}\n\n"

        # Add all other fields except those already handled
        excluded_fields = ['key', 'summary', 'description', 'points_of_contact',
                        'comments', 'last_comment', 'last_comment_date', 'teams', 'primary_team']
        for column, value in issue.items():
            if column not in excluded_fields and pd.notna(value):
                markdown += f"{prefix}  - {column}: {value}\n"

        markdown += "\n"
        return markdown

