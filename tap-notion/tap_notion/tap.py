"""Singer Tap entrypoint for Notion.

This module defines the TapNotion class, which is the entrypoint the Singer SDK
uses to run the tap. It declares:

- The tap name and configuration schema (settings users can provide).
- The list of streams which implement the Notion API endpoints.

Newcomers: Start here to see what configuration is supported and which streams
are exposed. See ARCHITECTURE.md in the repository for a full walkthrough of
how the pieces fit together (auth, pagination, state, and parent/child context).
"""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from . import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapNotion(Tap):
    """Singer Tap for Notion.

    Responsibilities:
    - Declare the tap name and the JSONSchema for configuration options.
    - Instantiate and return the list of available streams via `discover_streams`.

    Configuration is defined in `config_jsonschema` and includes:
    - auth_token (required): Notion integration token used for Bearer auth.
    - start_date (optional): Initial cutoff for incremental sync on the search stream.
    - notion_version, page_size, user_agent (optional): Header/behavior tweaks.
    - search_filter_object, search_query (optional): Convenience controls for /search.

    Stream relationships:
    - SearchStream emits page contexts consumed by PageDetailsStream and PageBlocksStream.
    - PagesIndexStream (non-incremental) is used by BlockChildrenStream to walk all blocks.

    See ARCHITECTURE.md for a full overview of how these pieces fit together.
    """

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
        """Instantiate and return the list of available streams.

        Stream overview and relationships:
        - UsersStream: Standalone list of users (GET /v1/users).
        - SearchStream: Workspace search (POST /v1/search), incremental on
          `last_edited_time`. Emits child context for pages only so that page
          streams can consume `{ "page_id": ... }`.
        - PageDetailsStream: Child of SearchStream, fetches page metadata
          (GET /v1/pages/{page_id}).
        - PageBlocksStream: Child of SearchStream, returns top-level blocks for
          each page (GET /v1/blocks/{page_id}/children). Emits child context for
          blocks with `has_children`.

        For full-block traversal, see BlockChildrenStream in streams.py which
        walks the entire block tree starting from page contexts.
        """
        return [
            streams.UsersStream(self),
            streams.SearchStream(self),
            streams.PageDetailsStream(self),
            streams.PageBlocksStream(self),
        ]


if __name__ == "__main__":
    TapNotion.cli()
