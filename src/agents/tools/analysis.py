import os
from datetime import datetime, timedelta
from typing import List
import pandas as pd
from crewai.tools import BaseTool
from pydantic import BaseModel, Field, ConfigDict

class JiraDataAnalysisSchema(BaseModel):
    """Schema for JiraStorageTools inputs"""
    action: str = Field(description="Action to perform. Possible actions: get_current_week, get_previous_week, list_available_weeks, get_data_for_month, read_weekly_data")
    base_path: str = Field(description="Base path of where the data is stored")
    year: int | None = Field(default=None, description="Year for get_data_for_month action (optional)")
    month: int | None = Field(default=None, description="Month for get_data_for_month action (optional)")
    week_name: str | None = Field(default=None, description="Week name for read_weekly_data action (optional)")

class JiraDataAnalysisTools(BaseTool):
    name: str = "Jira Data Analysis"
    description: str = "Tool for analyzing Jira data"
    model_config = ConfigDict(arbitrary_types_allowed=True)
    args_schema: type[BaseModel] = JiraDataAnalysisSchema

    def _run(self, action: str, base_path: str = "jira_weekly_data", year: int = None, month: int = None, week_name: str = None) -> str:
        """
        Execute data analysis actions

        Parameters:
        action (str): The action to perform. Possible actions: get_current_week, get_previous_week, list_available_weeks, get_data_for_month, read_weekly_data
        base_path (str): Base path of where the data is stored
        year (int): Year for get_data_for_month action
        month (int): Month for get_data_for_month action
        week_name (str): Week name for read_weekly_data action
        """

        actions = {
            "get_current_week": self._get_current_week,
            "get_previous_week": lambda: self._get_previous_week(base_path),
            "list_available_weeks": lambda: self._list_available_weeks(base_path),
            "get_data_for_month": lambda: self._get_data_for_month(base_path, year, month),
            "read_weekly_data": lambda: self._read_weekly_data(base_path, week_name),
        }

        if action not in actions:
            return f"Invalid action. Available actions: {list(actions.keys())}"

        return actions[action]()

    def _get_current_week(self) -> str:
        """Get current week number in YYYY-WXX format"""
        return datetime.now().strftime("%Y-W%W")

    def _get_previous_week(self, base_path: str) -> str:
        """Get previous week by checking available folders"""
        available_weeks = self._list_available_weeks(base_path)
        if not available_weeks:
            return "No available weeks found"
        return available_weeks[-2] if len(available_weeks) > 1 else "No previous week available"

    def _list_available_weeks(self, base_path: str) -> List[str]:
        """List all available weekly data folders"""
        if not os.path.exists(base_path):
            return []
        weeks = [d for d in os.listdir(base_path)
                if os.path.isdir(os.path.join(base_path, d))]
        return sorted(weeks)

    def _get_data_for_month(self, base_path: str, year: int, month: int) -> List[str]:
        """Retrieve all data folders for a specific month and year from the Jira weekly data."""
        if year is None or month is None:
            return "Year and month must be provided for get_data_for_month action"

        first_day_of_month = datetime(year, month, 1)
        if month == 12:
            last_day_of_month = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day_of_month = datetime(year, month + 1, 1) - timedelta(days=1)

        start_week = int(first_day_of_month.strftime("%W"))
        end_week = int(last_day_of_month.strftime("%W"))

        if not os.path.exists(base_path):
            return []

        available_weeks = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]
        filtered_weeks = [week for week in available_weeks if week.startswith(f"{year}-W") and start_week <= int(week.split("-W")[1]) <= end_week]

        return sorted(filtered_weeks)

    def _read_weekly_data(self, base_path: str, week_name: str) -> str:
        """Reads CSV data from a specified base path and week name, returning a DataFrame."""
        if week_name is None:
            return "Week name must be provided for read_weekly_data action"

        week_path = os.path.join(base_path, week_name)
        csv_file = os.path.join(week_path, "issues_data.csv")

        if not os.path.exists(csv_file):
            return f"CSV file not found: {csv_file}"

        df = pd.read_csv(csv_file)
        return f"Data loaded successfully. Shape: {df.shape}"
