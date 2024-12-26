from crewai.tools import BaseTool
from src.storage.data_validator import WeekDataValidator
from src.data.jira_data_fetcher import JiraDataFetcher

class JiraStorageTools(BaseTool):
    name: str = "Jira Storage Management"
    description: str = "Tools for managing Jira data storage and validation"
    
    def __init__(self):
        super().__init__()
        self.validator = WeekDataValidator()
        self.fetcher = JiraDataFetcher()
    
    def _run(self, action: str, **kwargs) -> str:
        """Execute storage management actions"""
        actions = {
            "current_week": self._get_current_week,
            "check_previous": self._check_previous_week,
            "check_current": self._check_current_week,
            "extract_data": self._extract_and_save,
            "validate_data": self._validate_extraction
        }
        
        if action not in actions:
            return "Invalid action requested"
            
        return actions[action](**kwargs)
    
    def _get_current_week(self, **kwargs):
        """Get current week information"""
        week = self.validator.get_current_week()
        return f"Current week is {week}"
    
    def _check_previous_week(self, **kwargs):
        """Check previous week data availability"""
        prev_week = self.validator.get_previous_week()
        if not prev_week:
            return "No previous week data available"
            
        valid, message = self.validator.check_week_data(prev_week)
        return f"Previous week {prev_week}: {message}"
    
    def _check_current_week(self, **kwargs):
        """Check if current week data exists"""
        week = self.validator.get_current_week()
        valid, message = self.validator.check_week_data(week)
        return f"Current week {week}: {message}"
    
    def _extract_and_save(self, project_id, labels, **kwargs):
        """Extract and save current week data"""
        try:
            issues = self.fetcher.get_weekly_jira_data(project_id, labels)
            week = self.fetcher.save_weekly_data(issues)
            return f"Data extracted and saved for week {week}"
        except Exception as e:
            return f"Error extracting data: {str(e)}"
    
    def _validate_extraction(self, **kwargs):
        """Validate extracted data quality"""
        week = self.validator.get_current_week()
        valid, metrics = self.validator.validate_data_quality(week)
        if not valid:
            return f"Validation failed: {metrics}"
        return f"Data validation successful: {metrics}"
