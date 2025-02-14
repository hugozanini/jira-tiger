from crewai.tools import BaseTool
from pydantic import BaseModel, Field, ConfigDict
import os
import json
import glob

class ListJiraReportsSchema(BaseModel):
    """Schema for ListJiraReportsSchema inputs"""
    action: str = Field(description="Action to perform. Possible actions: list_reports")
    path_to_markdown_files: str = Field(default=None, description="Path where the markdown files are saved")

class ListJiraReports(BaseTool):
    name: str = "ListJiraReports"
    description: str = "List jira markdown reports for a given folder "
    model_config = ConfigDict(arbitrary_types_allowed=True)
    args_schema: type[BaseModel] = ListJiraReportsSchema
    path_to_markdown_files: str = Field(default=None, description="Path where the markdown files are saved")

    def _run(self, action:str, path_to_markdown_files: str) -> str:
        """
        Execute actions
        """
        self.path_to_markdown_files = path_to_markdown_files
        actions = {
            "list_reports": lambda: self.list_markdown_reports(),
        }
        if action not in actions:
                return f"Invalid action. Available actions: {list(actions.keys())}"
        return actions[action]()

    def list_markdown_reports(self) -> str:
        """
        List all markdown files in the specified directory
        Returns:
            str: A string containing the list of markdown files, one per line
        """
        if not self.path_to_markdown_files or not os.path.exists(self.path_to_markdown_files):
            return f"Invalid path or path does not exist: {self.path_to_markdown_files}"

        markdown_files = []
        for file in os.listdir(self.path_to_markdown_files):
            if file.endswith(('.md', '.markdown')):
                markdown_files.append(file)
        if not markdown_files:
            return "No markdown files found in the specified directory."
        return "\n".join(markdown_files)


class ReadJiraReportSchema(BaseModel):
    """Schema for ReadJiraReportSchema inputs"""
    action: str = Field(description="Action to perform. Possible actions: read_report")
    file_path: str = Field(description="Full path to the markdown file to be read")

class ReadJiraReport(BaseTool):
    name: str = "ReadJiraReport"
    description: str = "Read contents of a specific Jira markdown report"
    model_config = ConfigDict(arbitrary_types_allowed=True)
    args_schema: type[BaseModel] = ReadJiraReportSchema
    file_path: str = Field(default=None, description="Full path to the markdown file to be read")

    def _run(self, action: str, file_path: str) -> str:
        """
        Execute actions
        """
        self.file_path = file_path
        actions = {
            "read_report": lambda: self.read_markdown_report(),
        }
        if action not in actions:
            return f"Invalid action. Available actions: {list(actions.keys())}"
        return actions[action]()

    def read_markdown_report(self) -> str:
        """
        Read the contents of a specific markdown file
        Returns:
            str: The contents of the markdown file or an error message
        """
        if not self.file_path or not os.path.exists(self.file_path):
            return f"Invalid file path or file does not exist: {self.file_path}"

        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            return content
        except Exception as e:
            return f"Error reading file: {str(e)}"

class SaveJiraDataSchema(BaseModel):
    """Schema for SaveJiraDataSchema inputs"""
    action: str = Field(description="Action to perform. Possible actions: save_data, generate_consolidated_report")
    data_dict: dict = Field(default=None, description="Dictionary containing the data to be saved")
    filename: str = Field(default=None, description="Name of the JSON file (e.g., 'data.json')")
    base_path: str = Field(default=None, description="Base path where the folder will be created")
    folder: str = Field(default=None, description="Folder name where the JSON file will be saved")

class SaveJiraData(BaseTool):
    name: str = "SaveJiraData"
    description: str = "Save JSON data to a specified location and generate consolidated reports"
    model_config = ConfigDict(arbitrary_types_allowed=True)
    args_schema: type[BaseModel] = SaveJiraDataSchema
    data_dict: dict = Field(default=None, description="Dictionary containing the data to be saved")
    filename: str = Field(default=None, description="Name of the JSON file")
    base_path: str = Field(default=None, description="Base path where the folder will be created")
    folder: str = Field(default=None, description="Folder name where the JSON file will be saved")

    def _run(self, action: str, data_dict: dict = None, filename: str = None, base_path: str = None, folder: str = None) -> str:
        """
        Execute actions
        """
        self.data_dict = data_dict
        self.filename = filename
        self.base_path = base_path
        self.folder = folder

        actions = {
            "save_data": lambda: self.save_json_data(),
            "generate_consolidated_report": lambda: self.generate_consolidated_report(),
        }
        if action not in actions:
            return f"Invalid action. Available actions: {list(actions.keys())}"
        return actions[action]()

    def save_json_data(self) -> str:
        """
        Save dictionary data as JSON file in the specified location
        Returns:
            str: Success message or error message
        """

        try:
            # Create full directory path
            full_dir_path = os.path.join(self.base_path, self.folder)

            # Create directory if it doesn't exist
            os.makedirs(full_dir_path, exist_ok=True)

            # Create full file path
            full_file_path = os.path.join(full_dir_path, self.filename)

            # Save the JSON file
            with open(full_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.data_dict, f, indent=4)

            return f"Data successfully saved to {full_file_path}"
        except Exception as e:
            return f"Error saving JSON data: {str(e)}"

    def generate_consolidated_report(self) -> str:
        """
        Generate a consolidated JSON report from all JSON files in the specified folder
        Returns:
            str: Path to the consolidated report or error message
        """

        input_directory = os.path.join(self.base_path, self.folder)
        if not os.path.exists(input_directory):
            return f"Directory does not exist: {input_directory}"

        output_file = os.path.join(self.base_path, self.folder, f"{self.folder}.json")

        # Initialize the consolidated structure
        consolidated_data = {
            "start_date": "<START_DATE_PLACEHOLDER>",
            "end_date": "<END_DATE_PLACEHOLDER>",
            "work_evolution": "<WORK_EVOLUTION_PLACEHOLDER>",
            "workstreams_summary": "<WORKSTREAMS_SUMMARY_PLACEHOLDER>",
            "teams": []
        }

        # Get all JSON files in the directory
        json_files = glob.glob(os.path.join(input_directory, '*.json'))

        # Process each JSON file
        for json_file in json_files:
            with open(json_file, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    team_data = {
                        "name": data.get("name", "Unknown"),
                        "contacts": data.get("contacts", []),
                        "updated_issues": data.get("updated_issues", []),
                        "no_update_issues": data.get("no_update_issues", [])
                    }
                    consolidated_data["teams"].append(team_data)
                except json.JSONDecodeError as e:
                    return f"Error reading {json_file}: {str(e)}"

        # Write consolidated data to output file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(consolidated_data, f, indent=4, ensure_ascii=False)

        return "Consolidated report saved at " + output_file

class JsonFileOperationsSchema(BaseModel):
    """Schema for JsonFileOperationsSchema inputs"""
    action: str = Field(description="Action to perform. Possible actions: read_json, save_json")
    file_path: str = Field(description="Full path to the JSON file")
    data: dict = Field(default=None, description="Dictionary data to save (only required for save_json action)")

class JsonFileOperations(BaseTool):
    name: str = "JsonFileOperations"
    description: str = "Read from or save to JSON files"
    model_config = ConfigDict(arbitrary_types_allowed=True)
    args_schema: type[BaseModel] = JsonFileOperationsSchema
    file_path: str = Field(default=None, description="Full path to the JSON file")
    data: dict = Field(default=None, description="Dictionary data to save")

    def _run(self, action: str, file_path: str, data: dict = None) -> str:
        """
        Execute actions
        """
        self.file_path = file_path
        self.data = data
        actions = {
            "read_json": lambda: self.read_json_file(),
            "save_json": lambda: self.save_json_file(),
        }
        if action not in actions:
            return f"Invalid action. Available actions: {list(actions.keys())}"
        return actions[action]()

    def read_json_file(self) -> str:
        """
        Read the contents of a JSON file
        Returns:
            str: The contents of the JSON file or an error message
        """
        if not self.file_path or not os.path.exists(self.file_path):
            return f"Invalid file path or file does not exist: {self.file_path}"

        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                content = json.load(file)
            return json.dumps(content, indent=4)
        except Exception as e:
            return f"Error reading JSON file: {str(e)}"

    def save_json_file(self) -> str:
        """
        Save dictionary data as JSON file. Skip if identical data already exists.
        Returns:
            str: Success message or error message
        """
        if not self.data:
            return "No data provided to save"

        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)

            # Check if file exists and has same content
            if os.path.exists(self.file_path):
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                if existing_data == self.data:
                    return f"Data successfully saved to {self.file_path}"

            # Save the JSON file if different or doesn't exist
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=4)

            return f"Data successfully saved to {self.file_path}"
        except Exception as e:
            return f"Error saving JSON file: {str(e)}"
