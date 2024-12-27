import pytest
from unittest.mock import patch, Mock
from src.data.jira_data_fetcher import JiraDataFetcher
import json
import os

@pytest.fixture
def jira_fetcher():
    """Setup test environment and return JiraDataFetcher instance"""
    with patch.dict(os.environ, {
        'JIRA_URL': 'https://test.atlassian.net',
        'JIRA_USERNAME': 'test_user',
        'JIRA_API_TOKEN': 'test_token'
    }):
        return JiraDataFetcher()

def test_get_labeled_issues(jira_fetcher):
    """Test fetching labeled issues"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "issues": [
            {"key": "TEST-1", "fields": {"summary": "Test Issue"}}
        ]
    }

    with patch('src.connection.jira_connection.JiraConnectionManager.make_request') as mock_request:
        mock_request.return_value = mock_response
        result = jira_fetcher.get_labeled_issues("TEST", ["test-label"])

        assert result is not None
        assert "issues" in result
        assert len(result["issues"]) == 1
        assert result["issues"][0]["key"] == "TEST-1"

def test_get_all_labeled_issues(jira_fetcher):
    """Test pagination for labeled issues"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "issues": [
            {"key": "TEST-1"},
            {"key": "TEST-2"}
        ]
    }

    with patch('src.connection.jira_connection.JiraConnectionManager.make_request') as mock_request:
        mock_request.return_value = mock_response
        result = jira_fetcher.get_all_labeled_issues("TEST", ["test-label"])

        assert len(result) == 2
        assert result[0]["key"] == "TEST-1"
        assert result[1]["key"] == "TEST-2"

def test_get_linked_issues(jira_fetcher):
    """Test linked issues extraction"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "fields": {
            "issuelinks": [
                {
                    "outwardIssue": {"key": "TEST-2"},
                    "type": {"name": "relates to"}
                }
            ]
        }
    }

    with patch('src.connection.jira_connection.JiraConnectionManager.make_request') as mock_request:
        mock_request.return_value = mock_response
        result = jira_fetcher.get_linked_issues("TEST-1")

        assert len(result) == 1
        assert result[0]["key"] == "TEST-2"
        assert result[0]["type"] == "relates to"

def test_get_clean_issue_info(jira_fetcher):
    """Test issue info cleaning and formatting"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "fields": {
            "summary": "Test Issue",
            "status": {"name": "Open"},
            "assignee": {"displayName": "Test User"},
            "description": {"content": [{"type": "paragraph", "content": [{"text": "Test description"}]}]},
            "labels": ["test-label"],
            "customfield_18723": "2024-12-31",
            "comment": {"comments": []},
            "issuelinks": []
        }
    }

    with patch('src.connection.jira_connection.JiraConnectionManager.make_request') as mock_request:
        mock_request.return_value = mock_response
        result = jira_fetcher.get_clean_issue_info("TEST-1")

        assert result is not None
        result_dict = json.loads(result)
        assert result_dict["basic_info"]["key"] == "TEST-1"
        assert result_dict["basic_info"]["status"] == "Open"

@pytest.mark.parametrize("issue_data,expected_count", [
    ({"key": "TEST-1", "fields": {"summary": "Test 1"}}, 1),
    ({"key": "TEST-2", "fields": {"summary": "Test 2"}}, 1),
])
def test_get_weekly_jira_data(jira_fetcher, issue_data, expected_count):
    """Test weekly data collection with different inputs"""
    mock_response = Mock()
    mock_response.status_code = 200

    responses = [
        {"issues": [issue_data]},  # get_all_labeled_issues
        {  # get_clean_issue_info
            "fields": {
                "summary": "Test Issue",
                "status": {"name": "Open"},
                "assignee": {"displayName": "Test User"},
                "description": {"content": []},
                "labels": ["test-label"],
                "issuelinks": []
            }
        },
        {"issues": []}  # get_child_issues
    ]

    with patch('src.connection.jira_connection.JiraConnectionManager.make_request') as mock_request:
        mock_request.return_value = mock_response
        mock_response.json.side_effect = responses

        result = jira_fetcher.get_weekly_jira_data("TEST", ["test-label"], limit=1)
        assert isinstance(result, list)
        assert len(result) == expected_count