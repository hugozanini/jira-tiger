"""Jira Tiger - A tool for efficient Jira data extraction and analysis"""
from .connection.jira_connection import JiraConnectionManager

__version__ = "0.1.0"
__author__ = "Hugo Zanini"

__all__ = ["JiraConnectionManager", "JiraDataFetcher"]