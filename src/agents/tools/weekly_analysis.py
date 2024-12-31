import pandas as pd
import ast
from datetime import datetime
import json
import os
from crewai.tools import BaseTool
from pydantic import BaseModel, Field, ConfigDict

class JiraWeeklyDataAnalysisSchema(BaseModel):
    """Schema for JiraStorageTools inputs"""
    action: str = Field(description="Action to perform. Possible actions: get_weekly_report")
    current_week_path: str = Field(default=None, description="Path for the current week data")
    previous_week_path: str = Field(default=None, description="Path for the previous week data")

class JiraWeeklyDataAnalysis(BaseTool):
    name: str = "Jira Weekly Data Analysis Tools"
    description: str = "Tool for analyze data extracted from Jira boards"
    model_config = ConfigDict(arbitrary_types_allowed=True)
    args_schema: type[BaseModel] = JiraWeeklyDataAnalysisSchema
    df_current_week: pd.DataFrame = Field(default_factory=pd.DataFrame)
    df_previous_week: pd.DataFrame = Field(default_factory=pd.DataFrame)
    current_week_path: str = "Current week path"
    previous_week_path: str = "Previous week path"

    def _run(self, action:str, current_week_path: str, previous_week_path: str) -> str:
        """
        Execute data analysis actions
        Parameters:
        action: str: The action to perform. Possible actions: "get_weekly_report"
        current_week_path (str): Path for the current week data.
        previous_week_path (str): Path for the previous week data.
        """

        actions = {
            "get_weekly_report": self._generate_markdown
        }

        if action not in actions:
            return f"Invalid action. Available actions: {list(actions.keys())}"

        self.current_week_path = current_week_path
        self.previous_week_path = previous_week_path
        self.df_current_week = None
        self.df_previous_week = None

        return actions[action]()


    def _generate_markdown(self) -> str:
        report_path, report = self.generate_markdown_report()
        return report_path #report

    def add_last_update_columns(self, df):
        """Adds last_update_date and last_update columns based on the most recent comment"""
        def extract_last_comment(comments_str):
            if pd.isna(comments_str) or comments_str == '[]':
                return pd.NA, pd.NA

            comments = ast.literal_eval(comments_str)

            if not comments:
                return pd.NA, pd.NA

            comments.sort(key=lambda x: datetime.strptime(x['created'], '%Y-%m-%dT%H:%M:%S.%f%z'))
            last_comment = comments[-1]

            last_date = datetime.strptime(last_comment['created'], '%Y-%m-%dT%H:%M:%S.%f%z').date()
            last_body = last_comment['body']

            return last_date, last_body

        df[['last_update_date', 'last_update']] = df['comments'].apply(
            lambda x: pd.Series(extract_last_comment(x))
        )

        return df

    def transform_project_dates(self, df):
        """Transforms project date columns by extracting start and end dates"""
        df['project_target_start'] = df['project_target'].apply(
            lambda x: json.loads(x)['start'] if pd.notna(x) else pd.NA
        )
        df['project_target_end'] = df['project_target'].apply(
            lambda x: json.loads(x)['end'] if pd.notna(x) else pd.NA
        )

        df['project_actual_start'] = df['project_start'].apply(
            lambda x: json.loads(x)['start'] if pd.notna(x) else pd.NA
        )
        df['project_projected_end'] = df['project_start'].apply(
            lambda x: json.loads(x)['end'] if pd.notna(x) else pd.NA
        )

        df.drop(columns=['project_target', 'project_start'], inplace=True)
        return df

    def create_parent_child_mapping(self, df):
        """Creates a mapping of parent issues and their child issues"""
        team_mapping = {}
        parent_issues = []
        child_lists = []
        teams = []

        for _, row in df.iterrows():
            if pd.notna(row['teams']):
                team_mapping[row['key']] = row['teams']

        for _, row in df.iterrows():
            if pd.isna(row['teams']) and pd.notna(row['parent_issue']):
                parent_team = team_mapping.get(row['parent_issue'])
                if parent_team:
                    team_mapping[row['key']] = parent_team

        parent_keys = df['key'].unique()

        for parent_key in parent_keys:
            children = df[df['parent_issue'] == parent_key]['key'].tolist()
            parent_row = df[df['key'] == parent_key].iloc[0]
            if children or pd.isna(parent_row['parent_issue']):
                parent_issues.append(parent_key)
                child_lists.append(children)
                teams.append(team_mapping.get(parent_key, ''))

        result_df = pd.DataFrame({
            'Issue Key': parent_issues,
            'Team': teams,
            'Child Issues': [', '.join(children) if children else '-' for children in child_lists]
        })

        return result_df

    def generate_markdown_report(self):
        """Generates the complete markdown report"""
        self.df_current_week = pd.read_csv(self.current_week_path)
        self.df_previous_week = pd.read_csv(self.previous_week_path)

        # Process dataframes
        self.df_current_week = self.add_last_update_columns(self.df_current_week)
        self.df_current_week = self.transform_project_dates(self.df_current_week)

        self.df_previous_week = self.add_last_update_columns(self.df_previous_week)
        self.df_previous_week = self.transform_project_dates(self.df_previous_week)

        report = []

        # Generate report content
        report.append("# Multi-repos Project Weekly Analysis Report")
        report.append(f"\nReport generated on: {datetime.now().strftime('%Y-%m-%d')}\n")

        report.append("## Overview")
        report.append("\n### Current Week Statistics")
        report.append(f"- Total issues: {len(self.df_current_week)}")
        report.append(f"- Parent issues: {len(self.df_current_week[pd.isna(self.df_current_week['parent_issue'])])}")
        report.append(f"- Child issues: {len(self.df_current_week[pd.notna(self.df_current_week['parent_issue'])])}")

        report.append("\n### Previous Week Statistics")
        report.append(f"- Total issues: {len(self.df_previous_week)}")
        report.append(f"- Parent issues: {len(self.df_previous_week[pd.isna(self.df_previous_week['parent_issue'])])}")
        report.append(f"- Child issues: {len(self.df_previous_week[pd.notna(self.df_previous_week['parent_issue'])])}\n")

        # Full datasets
        report.append("## Current Week Full Dataset")
        report.append("\nThis section contains all issues and their details from the current week:\n")
        report.append(self.df_current_week.to_markdown(index=False))

        report.append("\n## Previous Week Full Dataset")
        report.append("\nThis section contains all issues and their details from the previous week:\n")
        report.append(self.df_previous_week.to_markdown(index=False))

        # Parent-Child Mapping
        report.append("\n## Issue Hierarchy")
        report.append("\n### Current Week Hierarchy")
        parent_child_current = self.create_parent_child_mapping(self.df_current_week)
        report.append(parent_child_current.to_markdown(index=False))

        report.append("\n### Previous Week Hierarchy")
        parent_child_previous = self.create_parent_child_mapping(self.df_previous_week)
        report.append(parent_child_previous.to_markdown(index=False))

        # Save report
        output_path = 'multi_repos_analysis_report.md'
        with open(output_path, 'w') as f:
            f.write("\n".join(report))

        return os.path.abspath(output_path), report