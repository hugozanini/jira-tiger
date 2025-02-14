import pandas as pd
from datetime import datetime
import json
import os

class WeeklyDataManager:
    def __init__(self, base_path="jira_weekly_data"):
        self.base_path = base_path

    def save_weekly_data(self, data, week=None):
        """Save data with versioning"""
        week = week or datetime.now().strftime("%Y-W%W")
        folder_path = os.path.join(self.base_path, week)
        os.makedirs(folder_path, exist_ok=True)

        # Save raw data
        with open(f"{folder_path}/raw_data.json", "w") as f:
            json.dump(data, f, indent=2)

        # Save processed data
        df = pd.DataFrame(data)
        df.to_parquet(f"{folder_path}/processed_data.parquet")

        return week

    def get_weekly_data(self, week):
        """Retrieve data for specific week"""
        folder_path = os.path.join(self.base_path, week)

        try:
            df = pd.read_parquet(f"{folder_path}/processed_data.parquet")
            return df
        except FileNotFoundError:
            return None

    def list_available_weeks(self):
        """List all available weekly data"""
        if not os.path.exists(self.base_path):
            return []
        return sorted(os.listdir(self.base_path))
