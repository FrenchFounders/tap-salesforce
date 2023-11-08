"""REST client handling, including salesforceStream base class."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Iterable

import requests
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from simple_salesforce import SalesforceLogin

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class SalesforceStream(RESTStream):
    """salesforce stream class."""

    def __init__(self, tap):
        super().__init__(tap)
        self._credentials = None
        self._access_token = None
        self._instance_url = ''
        self._auth_header = None
        self.login_timer = None
        login = SalesforceLogin(
            username=self.config.get("username"),
            password=self.config.get("password"),
            security_token=self.config.get("security_token")
        )
        self._access_token, host = login
        self._instance_url = "https://" + host

    @property
    def rest_headers(self):
        return {"Authorization": "Bearer {}".format(self._access_token)}

    @property
    def get_credentials(self):
        return {
            "username": self.config.get("username"),
            "password": self.config.get("password"),
            "security_token": self.config.get("security_token")
        }

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self._instance_url

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self._access_token,
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = super().http_headers
        headers['Authorization'] = "Bearer {}".format(self._access_token)

        return headers

    def validate_response(self, response):
        # Still catch error status codes
        if response.status_code == 404:
            return

        super().validate_response(response)
