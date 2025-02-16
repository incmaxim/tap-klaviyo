"""REST client handling, including KlaviyoStream base class."""

from __future__ import annotations

import typing as t
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import parse_qsl
import base64
import requests
from typing import Optional

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.pagination import BaseHATEOASPaginator
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    from urllib.parse import ParseResult

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
UTC = timezone.utc
DEFAULT_START_DATE = datetime(2000, 1, 1, tzinfo=UTC).isoformat()


def _isodate_from_date_string(date_string: str) -> str:
    """Convert a date string to an ISO date string."""
    try:
        # Prvo pokušaj parse ISO formata
        dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
    except ValueError:
        # Ako ne uspije, pokušaj YYYY-MM-DD format
        dt = datetime.strptime(date_string, "%Y-%m-%d")
    return dt.replace(tzinfo=UTC).isoformat()


class KlaviyoPaginator(BaseHATEOASPaginator):
    """HATEOAS paginator for the Klaviyo API."""

    def get_next_url(self, response: requests.Response) -> str:
        data = response.json()
        return data.get("links").get("next")  # type: ignore[no-any-return]


class KlaviyoAuthenticator:
    """Handles OAuth authentication for Klaviyo API."""

    TOKEN_URL = "https://a.klaviyo.com/oauth/token"

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None

    def _get_basic_auth_header(self) -> str:
        """Generate Basic auth header from client credentials."""
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"

    def get_access_token(self) -> str:
        """Get a valid access token, refreshing if necessary."""
        if not self._access_token or not self._token_expires_at or datetime.utcnow() >= self._token_expires_at:
            self._refresh_access_token()
        return self._access_token

    def _refresh_access_token(self) -> None:
        """Refresh the access token using the refresh token."""
        try:
            headers = {
                "Authorization": self._get_basic_auth_header(),
                "Content-Type": "application/x-www-form-urlencoded"
            }
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token
            }

            response = requests.post(self.TOKEN_URL, headers=headers, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            if "access_token" not in token_data:
                raise ValueError("No access_token in response")
            
            self._access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)  # default to 1 hour
            self._token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in - 300)
        except Exception as e:
            raise Exception(f"Failed to refresh access token: {str(e)}") from e


class KlaviyoStream(RESTStream):
    """Klaviyo stream class."""

    url_base = "https://a.klaviyo.com/api"
    records_jsonpath = "$[data][*]"
    max_page_size: int | None = None
    _authenticator: Optional[KlaviyoAuthenticator] = None

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object."""
        if not self._authenticator:
            self._authenticator = KlaviyoAuthenticator(
                client_id=self.config["client_id"],
                client_secret=self.config["client_secret"],
                refresh_token=self.config["refresh_token"]
            )
        
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="Authorization",
            value=f"Bearer {self._authenticator.get_access_token()}",
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        if "revision" in self.config:
            headers["revision"] = self.config.get("revision")
        return headers

    def get_new_paginator(self) -> BaseHATEOASPaginator:
        return KlaviyoPaginator()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}

        if next_page_token:
            params.update(parse_qsl(next_page_token.query))

        if self.replication_key:
            if self.get_starting_timestamp(context):
                filter_timestamp = self.get_starting_timestamp(context)
            elif self.config.get("start_date"):
                filter_timestamp = _isodate_from_date_string(self.config.get("start_date"))
            else:
                filter_timestamp = DEFAULT_START_DATE

            if self.is_sorted:
                params["sort"] = self.replication_key

            params["filter"] = f"greater-than({self.replication_key},{filter_timestamp})"

        if self.max_page_size:
            params["page[size]"] = self.max_page_size
        return params

    def get_records(
        self,
        context: dict | None,
    ) -> t.Iterable[dict]:
        """Get records from stream source."""
        selected_streams = self.tap.config.get('selected_streams', [])
        if selected_streams and self.name not in selected_streams:
            self.logger.info(f"Stream {self.name} is not selected, skipping")
            return
        
        self.logger.info(f"Fetching records for stream {self.name}")
        yield from super().get_records(context)

    def get_selected_properties(self) -> set[str]:
        """Get set of selected property names."""
        if not self.selected:
            return set()
        
        properties = set()
        excluded_properties = set()
        
        for md in self.metadata.values():
            if md.key.startswith('!'):
                excluded_properties.add(md.key[1:])  # Makni ! prefix
            elif md.selected:
                properties.add(md.key)
        
        # Ukloni isključene propertije
        return properties - excluded_properties
