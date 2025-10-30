"""REST client handling and NotionStream base class.

This module defines the NotionStream class, a small specialization of the
Singer SDK's RESTStream tailored to the Notion API. It centralizes the
common behaviors for all streams in this tap:

- Base URL and HTTP headers (including Notion-Version and optional User-Agent).
- Authentication using a Notion integration token (Bearer auth).
- Default pagination and record extraction using the standard Notion envelope
  shape: `{ "results": [...], "next_cursor": "..." }`.
- Query parameter handling for GET endpoints, with page_size and start_cursor.
- Response parsing via JSONPath to yield individual records.

Downstream stream classes in `streams.py` inherit from NotionStream and only
provide endpoint-specific details like `path`, `rest_method`, schema, and any
special payload or child-context logic.
"""

from __future__ import annotations

import decimal
import sys
import typing as t
from importlib import resources

from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context


# Directory for optional JSON schema files (not used in this tap by default)
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class NotionStream(RESTStream):
    """Base stream for the Notion API.

    Implements Notion-specific defaults for base URL, pagination, and headers.
    """

    # Most list endpoints return an envelope with `results` and `next_cursor`.
    records_jsonpath = "$.results[*]"
    next_page_token_jsonpath = "$.next_cursor"  # noqa: S105

    @override
    @property
    def url_base(self) -> str:
        """Return the Notion API base URL."""
        # Notion API base is fixed; no trailing slash here.
        return "https://api.notion.com/v1"

    @override
    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object using the Notion integration token."""
        return BearerTokenAuthenticator(token=self.config.get("auth_token", ""))

    @property
    @override
    def http_headers(self) -> dict:
        """Return the HTTP headers including Notion-Version and optional UA."""
        headers: dict[str, str] = {}
        notion_version = self.config.get("notion_version") or "2022-06-28"
        headers["Notion-Version"] = notion_version
        # Optional custom User-Agent
        user_agent = self.config.get("user_agent")
        if user_agent:
            headers["User-Agent"] = user_agent
        return headers

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for Notion list endpoints.

        This method builds the query parameters (the part after `?` in a URL) for API
        requests, handling pagination and page size.

        How it works:
        1. The SDK calls this automatically before each request
        2. For GET requests, adds pagination params to the URL:
           - First request: ?page_size=100
           - Subsequent requests: ?page_size=100&start_cursor=abc123
        3. For POST requests (like /v1/search), returns empty params dict since
           Notion expects cursor & page_size in the JSON body instead
        4. The SDK loops through pages until next_cursor is null in the response

        Args:
            context: The stream context.
            next_page_token: The next cursor value from the previous response, or None
                for the first request.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict[str, t.Any] = {}
        # For POST endpoints (e.g., /v1/search), Notion expects cursor & page_size in the JSON body
        if getattr(self, "rest_method", "GET").upper() != "POST":
            if next_page_token:
                params["start_cursor"] = next_page_token
            # Default to max page size for speed, unless explicitly provided
            page_size = self.config.get("page_size") or 100
            params["page_size"] = page_size
        return params

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        # Default: No payload. Override in POST-based streams to provide a body.
        return None

    @override
    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        This method extracts individual records from the API response and yields them
        one at a time.

        How it works:
        1. Called automatically by the SDK after receiving an HTTP response
        2. Converts the response body to JSON (using Decimal for precise numbers)
        3. Applies the records_jsonpath ("$.results[*]") to extract items from the
           results array
        4. Yields each record individually (memory efficient for large datasets)

        Example:
            API response: {"results": [{"id": "1"}, {"id": "2"}], "next_cursor": "abc"}
            This method yields: {"id": "1"}, then {"id": "2"}

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Note: As of SDK v0.47.0, this method is automatically executed for all stream types.
        You should not need to call this method directly in custom `get_records` implementations.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        return row
