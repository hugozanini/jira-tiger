from datetime import datetime
import os
import json
import pandas as pd

class DataValidator:
    def __init__(self):
        self.required_columns = [
            "key", "summary", "status", "assignee",
            "points_of_contact", "teams"
        ]

    def validate_data(self, data):
        """Validate data structure and content"""
        if not data:
            return False, "Empty data"

        df = pd.DataFrame(data)

        # Check required columns
        missing_cols = [col for col in self.required_columns 
                       if col not in df.columns]
        if missing_cols:
            return False, f"Missing columns: {missing_cols}"

        return True, "Data valid"

class WeekDataValidator:
    def __init__(self, base_path="jira_weekly_data"):
        self.base_path = base_path

    def get_current_week(self):
        """Get current week number in YYYY-WXX format"""
        return datetime.now().strftime("%Y-W%W")

    def get_previous_week(self):
        """Get previous week by checking available folders"""
        available_weeks = self.list_available_weeks()
        if not available_weeks:
            return None
        return available_weeks[-2] if len(available_weeks) > 1 else None

    def check_week_data(self, week):
        """Verify if week data exists and is valid"""
        folder_path = os.path.join(self.base_path, week)
        if not os.path.exists(folder_path):
            return False, "Week data folder not found"

        required_files = ['raw_data.json', 'issues_data.csv']
        missing_files = [f for f in required_files
                        if not os.path.exists(os.path.join(folder_path, f))]

        if missing_files:
            return False, f"Missing files: {missing_files}"

        return True, "There is already data extracted for the " + week + " week"

    def validate_data_quality(self, week):
        """Check data quality metrics"""
        folder_path = os.path.join(self.base_path, week)
        try:
            df = pd.read_csv(os.path.join(folder_path, 'issues_data.csv'))
            metrics = {
                "total_issues": len(df),
                "missing_values": df.isnull().sum().to_dict(),
                "status_distribution": df['status'].value_counts().to_dict()
            }
            return True, metrics
        except Exception as e:
            return False, str(e)

    def list_available_weeks(self):
        """List all available weekly data folders"""
        if not os.path.exists(self.base_path):
            return []
        weeks = [d for d in os.listdir(self.base_path)
                if os.path.isdir(os.path.join(self.base_path, d))]
        return sorted(weeks)