from crewai.tools import BaseTool
from src.storage.data_validator import WeekDataValidator
from src.data.jira_data_fetcher import JiraDataFetcher
from datetime import datetime
from typing import Type
from pydantic import BaseModel, Field, ConfigDict

class JiraStorageTools(BaseTool):
    name: str = "Jira Storage Management"
    description: str = "Tool for managing and validating weekly Jira data extractions"
    model_config = ConfigDict(arbitrary_types_allowed=True)
    validator: WeekDataValidator = Field(default_factory=WeekDataValidator)
    fetcher: JiraDataFetcher = Field(default_factory=JiraDataFetcher)

    def _run(self, action: str, project_id: str = "", labels: list = []) -> str:
        """Execute storage management actions
        Parameters:
        * action -> Action to perform: current_week, check_previous, check_current, extract_data, validate_data
        * project_id -> Jira project ID for data extraction
        * labels -> List of labels to filter Jira issues
        """
        # self.validator = WeekDataValidator()
        self.fetcher = JiraDataFetcher()
        self.validator = WeekDataValidator()

        actions = {
            "current_week": self._get_current_week,
            "check_previous": self._check_previous_week,
            "check_current": self._check_current_week,
            "extract_data": lambda: self._extract_and_save(project_id, labels),
            "validate_data": self._validate_extraction
        }

        if action not in actions:
            return f"Invalid action. Available actions: {list(actions.keys())}"

        return actions[action]()

    def _get_current_week(self) -> str:
        """Get current week information"""
        week = datetime.now().strftime("%Y-W%W")
        return f"Current week is {week}"

    def _check_previous_week(self) -> str:
        """Check previous week data availability"""
        prev_week = self.validator.get_previous_week()
        if not prev_week:
            return "No previous week data available"

        valid, message = self.validator.check_week_data(prev_week)
        return f"Previous week {prev_week}: {message}"

    def _check_current_week(self) -> str:
        """Check if current week data exists"""
        week = self.validator.get_current_week()
        valid, message = self.validator.check_week_data(week)
        return f"Current week {week}: {message}"

    def _extract_and_save(self, project_id: str, labels: list) -> str:
        """Extract and save current week data"""
        try:
            if not project_id or not labels:
                return "Error: project_id and labels are required for data extraction"

            issues = self.fetcher.get_weekly_jira_data(project_id, labels)
            week = self.fetcher.save_weekly_data(issues)
            return f"Data extracted and saved for week {week}"
        except Exception as e:
            return f"Error extracting data: {str(e)}"

    def _validate_extraction(self) -> str:
        """Validate extracted data quality"""
        week = self.validator.get_current_week()
        valid, metrics = self.validator.validate_data_quality(week)
        if not valid:
            return f"Validation failed: {metrics}"
        return f"Data validation successful: {metrics}"