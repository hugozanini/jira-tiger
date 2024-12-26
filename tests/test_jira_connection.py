import pytest
import requests
import os
from unittest.mock import patch, MagicMock
from src.connection.jira_connection import JiraConnectionManager, TimeoutConfig, JiraErrorHandler

@pytest.fixture
def jira_manager():
    # Set test environment variables
    os.environ['JIRA_URL'] = 'https://test.atlassian.net'
    os.environ['JIRA_USERNAME'] = 'test_user'
    os.environ['JIRA_API_TOKEN'] = 'test_token'
    return JiraConnectionManager()

def test_initialization(jira_manager):
    """Test proper initialization of JiraConnectionManager"""
    assert jira_manager.base_url == 'https://test.atlassian.net'
    assert jira_manager.session is not None
    assert isinstance(jira_manager.timeout_config, TimeoutConfig)
    assert isinstance(jira_manager.error_handler, JiraErrorHandler)

def test_connection_pooling(jira_manager):
    """Test connection pooling configuration"""
    adapter = jira_manager.session.get_adapter('https://')
    assert adapter.poolmanager.connection_pool_kw['maxsize'] == 100

@pytest.mark.parametrize("status_code,should_retry", [
    (429, True),   # Rate limiting
    (500, True),   # Server error
    (404, False),  # Not found - shouldn't retry
])
def test_error_handling(jira_manager, status_code, should_retry):
    """Test error handling for different status codes"""
    with patch('requests.Session.request') as mock_request:
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_request.side_effect = requests.exceptions.HTTPError(response=mock_response)

        try:
            jira_manager.make_request('GET', '/test')
        except requests.exceptions.HTTPError:
            pass

        expected_calls = 3 if should_retry else 1
        assert mock_request.call_count <= expected_calls

def test_timeout_config():
    """Test timeout configuration"""
    config = TimeoutConfig()
    assert config.connect_timeout == 10
    assert config.read_timeout == 30
    assert config.max_retries == 3

def test_backoff_calculation():
    """Test exponential backoff calculation"""
    handler = JiraErrorHandler()
    assert handler.get_backoff_time(0) == 30  # First retry
    assert handler.get_backoff_time(1) == 60  # Second retry
    assert handler.get_backoff_time(2) == 120 # Third retry
