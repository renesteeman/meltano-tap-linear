"""Notion tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_notion import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapNotion(Tap):
    """Notion tap class."""

    name = "tap-notion"

    # Notion configuration
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Integration token from Notion
            title="Auth Token",
            description="The Notion integration token (starts with 'secret_').",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description=(
                "Initial cutoff timestamp for incremental sync on the 'search' stream. "
                "Client-side filter using last_edited_time; subsequent runs use saved state."
            ),
        ),
        th.Property(
            "notion_version",
            th.StringType(nullable=True),
            title="Notion API Version",
            description="Override the Notion-Version header (default '2022-06-28').",
        ),
        th.Property(
            "page_size",
            th.IntegerType(nullable=True),
            description="Items per page for list/search endpoints (max 100).",
        ),
        th.Property(
            "user_agent",
            th.StringType(nullable=True),
            description=(
                "A custom User-Agent header to send with each request. Default is "
                "'<tap_name>/<tap_version>'"
            ),
        ),
        th.Property(
            "search_filter_object",
            th.StringType(nullable=True),
            description="Optional search filter object type: 'page' or 'database'.",
        ),
        th.Property(
            "search_query",
            th.StringType(nullable=True),
            description="Optional search query string.",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.NotionStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.UsersStream(self),
            streams.SearchStream(self),
        ]


if __name__ == "__main__":
    TapNotion.cli()
