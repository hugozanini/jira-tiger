from crewai.tools import BaseTool
from pydantic import BaseModel, Field, ConfigDict
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from typing import Dict, Any
import os
import json

class SlackMessageSchema(BaseModel):
    """Schema for SlackMessage inputs"""
    action: str = Field(description="Action to perform. Possible actions: send_message, consolidate_report")
    channel_id: str = Field(default=None, description="Slack channel ID to send the message to")
    report_data: Dict[str, Any] = Field(default=None, description="For send_message: path to the consolidated report file. For consolidate_report: dictionary with report paths and output path")
    report_file_path: str = Field(default=None, description="Path to the JSON file containing the report data to be sent to Slack")

class SlackMessage(BaseTool):
    name: str = "SlackMessage"
    description: str = "Tools for sending messages to Slack"
    model_config = ConfigDict(arbitrary_types_allowed=True)
    args_schema: type[BaseModel] = SlackMessageSchema
    channel_id: str = Field(default=None, description="Slack channel ID to send the message to")
    report_data: Dict[str, Any] = Field(default=None, description="For send_message: path to the consolidated report file. For consolidate_report: dictionary with report paths and output path")
    report_file_path: str = Field(default=None, description="Path to the JSON file containing the report data to be sent to Slack")


    def _run(self, action: str, channel_id: str, report_data: Dict[str, Any], report_file_path: str) -> str:
        """
        Execute Slack message actions
        """
        actions = {
            "send_message": lambda: self.send_message(channel_id, report_file_path),
            "consolidate_report": lambda: self.consolidate_report(report_data),
        }
        if action not in actions:
            return f"Invalid action. Available actions: {list(actions.keys())}"
        return actions[action]()

    def send_message(self, channel_id: str, report_file_path: str) -> Dict:
        """
        Posts a report to Slack with:
        1. Work Evolution Overview
        2. Workstreams Overview
        3. Team-wise Updates header
        4. Individual team updates in separate threads

        Args:
            channel_id: Slack channel ID
            report_file_path: Path to the consolidated report JSON file
        """
        # Read the report data from file
        with open(report_file_path, 'r') as f:
            report_data = json.load(f)

        slack_token = os.getenv('SLACK_BOT_TOKEN')
        if not slack_token:
            raise ValueError("SLACK_BOT_TOKEN environment variable not set")

        client = WebClient(token=slack_token)

        # Blocks for the Work Evolution Overview
        overview_blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🔔 Weekly Report: {report_data['start_date']} - {report_data['end_date']} 🔔",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Work Evolution Overview*"
                }
            },
            {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": report_data['work_evolution']
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Workstreams Overview*"
            },
            "accessory": {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": ":jira: Releases roadmap",
                    "emoji": True
                },
                "url": "https://company.atlassian.net/jira/discovery/share/views/df222e7",
                "action_id": "releases_roadmap_button"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": report_data['workstreams_summary']
            }
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "This message was automatically generated by :tiger-1: <https://company.enterprise.slack.com/team/U0882B6V7PY|Jira Tiger> AI Agents"
                }
            ]
        }
        ]
        # Header blocks for Team-wise Updates
        team_header_blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*👥 Team-wise Updates*"
            },
            "accessory": {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": ":jira: Teams roadmap",
                    "emoji": True
                },
                "url": "https://company.atlassian.net/jira/discovery/share/views/222e7",
                "action_id": "teams_roadmap_button"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "plain_text",
                "text": "Check the status of your teams' Jira issues below. If updates are needed, please make them and react to the message with ✅ ",
                "emoji": True
            }
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "This message was automatically generated by :tiger-1: <https://company.enterprise.slack.com/team/U0882B6V7PY|Jira Tiger> AI Agents"
                }
            ]
        }
        ]

        try:
            # Send the overview message
            overview_response = client.chat_postMessage(
                channel=channel_id,
                blocks=overview_blocks
            )

            # Send the team updates header
            team_header_response = client.chat_postMessage(
                channel=channel_id,
                blocks=team_header_blocks,
                thread_ts=overview_response["ts"]
            )

            # Send individual team updates in the thread
            team_responses = []
            for team in report_data['teams']:
                team_blocks = [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*{team['name']}:*\nPoints of contact: {', '.join(team['contacts'])}"
                        }
                    }
                ]

                # Add updated issues
                if team['updated_issues']:
                    for issue in team['updated_issues']:
                        issue_text = f"• <{issue['url']}|*{issue['id']}*> - {issue['title']}"
                        if issue.get('fup'):
                            issue_text += f"\n> {issue['fup']}"

                        team_blocks.append({
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": issue_text
                            }
                        })

                # Add non-updated issues
                if team['no_update_issues']:
                    for issue in team['no_update_issues']:
                        issue_text = f"• <{issue['url']}|*{issue['id']}*> - {issue['title']}"
                        if issue.get('fup'):
                            issue_text += f"\n> {issue['fup']}"

                        team_blocks.append({
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": issue_text
                            }
                        })

                # Send team update in the thread
                team_response = client.chat_postMessage(
                    channel=channel_id,
                    blocks=team_blocks,
                    thread_ts=overview_response["ts"]
                )
                team_responses.append(team_response)

            return {
                "overview_response": overview_response,
                "team_header_response": team_header_response,
                "team_responses": team_responses
            }

        except SlackApiError as e:
            return {"error": str(e.response["error"])}

    def consolidate_report(self, report_data: Dict[str, Any]) -> str:
        """
        Consolidates two report files into a single report file.

        Args:
            channel_id: Not used for this action
            report_data: Dictionary containing:
                - teams_report_path: Path to the teams report JSON
                - summary_report_path: Path to the summary report JSON
                - output_path: Path where to save the consolidated report

        Returns:
            str: Path to the consolidated report file
        """
        required_keys = ['teams_report_path', 'summary_report_path', 'output_path']
        if not all(key in report_data for key in required_keys):
            raise ValueError(f"Missing required keys in report_data. Required: {required_keys}")

        # Read the teams report
        with open(report_data['teams_report_path'], 'r') as f:
            teams_report = json.load(f)

        # Read the summary report
        with open(report_data['summary_report_path'], 'r') as f:
            summary_report = json.load(f)

        # Replace placeholders in teams report with summary data
        teams_report['start_date'] = summary_report['start_date']
        teams_report['end_date'] = summary_report['end_date']
        teams_report['work_evolution'] = summary_report['work_evolution']
        teams_report['workstreams_summary'] = summary_report['workstreams_summary']

        # Write consolidated report to output path
        os.makedirs(os.path.dirname(report_data['output_path']), exist_ok=True)
        with open(report_data['output_path'], 'w') as f:
            json.dump(teams_report, f, indent=4)

        return report_data['output_path']