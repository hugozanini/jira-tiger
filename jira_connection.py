import requests
from requests.adapters import HTTPAdapter, Retry
from requests.auth import HTTPBasicAuth
from typing import Optional, Dict, Any
import os
from datetime import datetime
import logging
import time

class JiraConnectionManager:
    def __init__(self):
        self.timeout_config = TimeoutConfig()
        self.error_handler = JiraErrorHandler()
        self._setup_logging()
        self.logger = JiraLogger().logger
        self.session = self._create_session()
        self.base_url = os.getenv('JIRA_URL')
        self.auth = HTTPBasicAuth(
            os.getenv('JIRA_USERNAME'),
            os.getenv('JIRA_API_TOKEN')
        )
        self._validate_config()

    def _setup_logging(self):
        """Configure logging for JiraConnectionManager"""
        logger = logging.getLogger(__name__)
        if not logger.handlers:  # Only add handlers if they don't exist
            file_handler = logging.FileHandler('jira_connection.log')
            console_handler = logging.StreamHandler()

            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
            logger.setLevel(logging.INFO)

        self.logger = logger


    def _create_session(self) -> requests.Session:
        session = requests.Session()
        # Configure connection pooling with increased limits
        adapter = self._configure_connection_pool()
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set default timeouts
        session.timeout = (self.timeout_config.connect_timeout,
                        self.timeout_config.read_timeout)

        return session


    def make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{endpoint}"
        retry_count = 0

        while retry_count < self.timeout_config.max_retries:
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    auth=self.auth,
                    timeout=(self.timeout_config.connect_timeout, 
                            self.timeout_config.read_timeout),
                    **kwargs
                )
                response.raise_for_status()
                return response

            except requests.exceptions.HTTPError as e:
                # Don't retry client errors (4xx)
                if 400 <= e.response.status_code < 500:
                    self.logger.error(f"Client error {e.response.status_code}: {str(e)}")
                    raise
                # Only retry server errors (5xx)
                if self.error_handler.handle_request_error(e, retry_count):
                    retry_count += 1
                    continue
                self.logger.error(f"Request failed after {retry_count} retries: {str(e)}")
                raise
            except requests.exceptions.RequestException as e:
                if self.error_handler.handle_request_error(e, retry_count):
                    retry_count += 1
                    continue
                self.logger.error(f"Request failed after {retry_count} retries: {str(e)}")
                raise

    def _configure_connection_pool(self):
        '''Configure connection pooling for the session'''
        adapter = HTTPAdapter(
            pool_connections=100,  # Number of connection pools to cache
            pool_maxsize=100,     # Maximum number of connections to save in the pool
            max_retries=Retry(
                total=5,
                backoff_factor=1,
                status_forcelist=[408, 429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
            )
        )
        return adapter

    def _validate_config(self):
        required_vars = ['JIRA_URL', 'JIRA_USERNAME', 'JIRA_API_TOKEN']
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {missing}")


class TimeoutConfig:
    # Timeout configuration for Jira API requests
    def __init__(self):
        self.connect_timeout = 10  # Connection timeout
        self.read_timeout = 30     # Read timeout
        self.max_retries = 3       # Maximum number of retries


class JiraErrorHandler:
    def __init__(self):
        self.max_retries = 3
        self.backoff_base = 30
        self.max_backoff = 300

    def handle_request_error(self, error, retry_count=0):
        """Handle request errors with exponential backoff"""
        if retry_count >= self.max_retries:
            return False

        if isinstance(error, (requests.exceptions.Timeout,
                            requests.exceptions.ConnectionError,
                            requests.exceptions.HTTPError)):
            if hasattr(error, 'response') and error.response.status_code == 429:
                # Handle rate limiting specifically
                retry_after = int(error.response.headers.get('Retry-After', self.backoff_base))
                time.sleep(retry_after)
                return True

            time.sleep(self.get_backoff_time(retry_count))  # Changed from _calculate_backoff
            return True
        return False

    def get_backoff_time(self, retry_count):  # Changed from _calculate_backoff
        """Calculate exponential backoff time"""
        backoff = min(self.max_backoff, self.backoff_base * (2 ** retry_count))
        return backoff


class JiraLogger:
    def __init__(self):
        self.logger = logging.getLogger('jira_client')
        self.logger.setLevel(logging.INFO)

        # File handler
        fh = logging.FileHandler('jira_client.log')
        fh.setLevel(logging.INFO)

        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        # Formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)