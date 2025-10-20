"""REST client handling, including NotionStream base class."""

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


# TODO: Delete this is if not using json files for schema definition
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
    def get_new_paginator(self) -> BaseAPIPaginator | None:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance, or ``None`` to indicate pagination
            is not supported.
        """
        return super().get_new_paginator()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for Notion list endpoints.

        Args:
            context: The stream context.
            next_page_token: The next cursor value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict[str, t.Any] = {}
        if next_page_token:
            params["start_cursor"] = next_page_token
        page_size = self.config.get("page_size")
        if page_size:
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
        # TODO: Delete this method if no payload is required. (Most REST APIs.)
        return None

    @override
    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
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
        # TODO: Delete this method if not needed.
        return row
