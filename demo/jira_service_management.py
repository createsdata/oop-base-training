import json
import requests

class JiraServiceManagement:
    """
    A class to interact with Jira Service Management via webhooks.

    Attributes:
    -----------
    webhook_url : str
        The webhook URL for posting issues to Jira Service Management.

    Methods:
    --------
    __init__(webhook_url)
        Initializes the JiraServiceManagement class with the given webhook URL.
    
    post_issue(title, description)
        Posts an issue to Jira Service Management with the given title and description.
    """

    def __init__(self, webhook_url):
        """
        Initializes the JiraServiceManagement class with the provided webhook URL.

        Parameters:
        -----------
        webhook_url : str
            The webhook URL to be used for posting issues to Jira.
        """
        self.webhook_url = webhook_url


    def post_issue(self, title, description):
        """
        Posts an issue to Jira Service Management with the specified title and description.

        Parameters:
        -----------
        title : str
            The title of the issue.
        description : str
            The description of the issue.

        Returns:
        --------
        None
        """
        headers = {
            "Content-type": "application/json"
        }

        payload = {
            "data": {
                "title": title, # Title of JIRA issue
                "description": description  # Description of JIRA issue
            }
        }

        # Sending POST request to Jira Service Management
        requests.post(
            url=self.webhook_url,
            headers=headers,
            data=json.dumps(payload)  # Converts the payload to JSON format
        )

