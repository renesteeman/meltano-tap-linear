"""Stream classes implementing Notion API endpoints.

This module contains the concrete stream implementations for the tap. All
streams inherit from `NotionStream` (see client.py) which handles base URL,
headers, authentication, and default pagination on Notion's standard envelope.

Use this file to understand:
- Which endpoints are covered and how they map to streams.
- How parent/child stream contexts propagate `page_id`/`block_id`.
- Where incremental cutoffs are applied (SearchStream) and where full traversal
  is performed (BlockChildrenStream).
"""

from __future__ import annotations

import typing as t
from importlib import resources
import datetime
import requests

from singer_sdk import typing as th  # JSON Schema typing helpers

from .client import NotionStream

# JSON schemas directory (unused currently but kept for future use)
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class UsersStream(NotionStream):
    """Notion users stream (GET /v1/users).

    - Endpoint: GET /v1/users
    - Pagination: Standard Notion envelope with `results` and `next_cursor`.
    - Schema: Basic user profile fields plus `person`/`bot` variants.
    - Keys: Primary key is `id`.

    Usage: This stream is independent and does not require any parent context.
    """

    # Stream identifier used in Singer tap operations and output
    name = "users"
    # API endpoint path that will be combined with the base URL
    path = "/users"
    # Field(s) that uniquely identify each record in the stream
    primary_keys: t.ClassVar[list[str]] = ["id"]
    # No incremental sync field; uses full table replication on every sync
    replication_key = None

    # Define the JSON schema for the users stream with all available fields
    schema = th.PropertiesList(
        # Type of Notion object (always "user" for this stream)
        th.Property("object", th.StringType),
        # Unique identifier for the user
        th.Property("id", th.StringType, description="User ID"),
        # Type of user: "person" or "bot"
        th.Property("type", th.StringType),
        # Display name of the user
        th.Property("name", th.StringType),
        # Person-specific properties (only present for human users)
        th.Property(
            "person",
            th.ObjectType(
                # Email address associated with the person user
                th.Property("email", th.StringType),
            ),
        ),
        # Bot-specific properties (only present for bot users)
        th.Property(
            "bot",
            th.ObjectType(
                # Information about who owns/created the bot
                th.Property("owner", th.ObjectType()),
            ),
        ),
    ).to_dict()


class SearchStream(NotionStream):
    """Workspace search stream (POST /v1/search).

    Implements incremental sync using last_edited_time and respects
    config start_date or stored state as the cutoff.
    """

    # Stream identifier used in Singer tap operations and output
    name = "search"
    # API endpoint path that will be combined with the base URL
    path = "/search"
    # Field(s) that uniquely identify each record in the stream
    primary_keys: t.ClassVar[list[str]] = ["id"]
    # Field used for incremental sync to track changes since last run
    replication_key = "last_edited_time"
    # HTTP method used for this endpoint (POST instead of GET)
    rest_method = "POST"

    # Define the JSON schema for the search stream with all available fields
    # Search endpoint returns results array and next_cursor for pagination
    schema = th.PropertiesList(
        # Type of Notion object (page, database, etc.)
        th.Property("object", th.StringType),
        # Unique identifier for the object
        th.Property("id", th.StringType),
        # URL to access the object in Notion
        th.Property("url", th.StringType),
        # Timestamp when the object was created
        th.Property("created_time", th.DateTimeType),
        # Timestamp when the object was last modified (used for incremental sync)
        th.Property("last_edited_time", th.DateTimeType),
        # Whether the object has been archived/deleted
        th.Property("archived", th.BooleanType),
        # Custom properties defined for the object (pages, databases, etc.)
        th.Property("properties", th.ObjectType()),
        # Parent object information (workspace, database, or page)
        th.Property("parent", th.ObjectType()),
    ).to_dict()

    def prepare_request_payload(
            self,
            context: dict | None,
            next_page_token: t.Any | None,
    ) -> dict | None:
        """Compose the POST body for Notion search.

        Notion expects cursor and page_size in the JSON body for POST endpoints.
        This method also applies optional filter and query settings from config
        and ensures results are sorted newest-first to align with the
        incremental cutoff logic used by this stream.
        """
        # Initialize empty dictionary to build the request body
        payload: dict[str, t.Any] = {}

        # Add pagination cursor to retrieve the next batch of results
        # Notion uses start_cursor in the request body (not query params) for POST endpoints
        if next_page_token:
            payload["start_cursor"] = next_page_token

        # Optionally restrict results to a specific Notion object type via config
        # Filters results to only "page", "database", etc. if specified
        filter_object = self.config.get("search_filter_object")  # e.g. "page" or "database"
        if filter_object:
            payload["filter"] = {"property": "object", "value": filter_object}

        # Optionally search for objects containing specific text in their content
        # If not provided, returns all accessible objects regardless of text content
        query = self.config.get("search_query")
        if query:
            payload["query"] = query

        # Set the maximum number of results to return in a single request
        # Defaults to 100 for optimal speed; config can override for different needs
        page_size = self.config.get("page_size") or 100
        payload["page_size"] = page_size

        # Sort results by last modification time in descending order (newest first)
        # Critical for incremental sync: allows early termination when older records appear
        payload["sort"] = {"timestamp": "last_edited_time", "direction": "descending"}

        # Return the complete request body dictionary
        return payload or {}

    # --- Incremental filtering helpers ---
    @staticmethod
    def _parse_iso8601(timestamp: str):
        """Parse ISO 8601 timestamp strings into timezone-aware datetime objects.

        Handles multiple timestamp formats:
        - Date-only: "YYYY-MM-DD"
        - ISO 8601 with Z: "2024-01-15T10:30:00Z"
        - ISO 8601 with offset: "2024-01-15T10:30:00+00:00"
        - ISO 8601 without timezone: "2024-01-15T10:30:00"

        Always returns datetime with UTC timezone for consistent comparison.
        """
        # Remove leading/trailing whitespace from timestamp string
        timestamp_str = timestamp.strip()

        # Handle date-only strings in YYYY-MM-DD format (10 characters, no 'T')
        # Convert to datetime at midnight UTC for consistent comparisons
        if len(timestamp_str) == 10 and "T" not in timestamp_str:
            date_obj = datetime.date.fromisoformat(timestamp_str)
            return datetime.datetime(date_obj.year, date_obj.month, date_obj.day, tzinfo=datetime.timezone.utc)

        # Normalize trailing 'Z' to explicit UTC offset format for fromisoformat compatibility
        # Python's fromisoformat requires "+00:00" instead of "Z" for UTC designation
        if timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str[:-1] + "+00:00"

        # Parse the ISO 8601 formatted string into a datetime object
        datetime_object = datetime.datetime.fromisoformat(timestamp_str)

        # Ensure all timestamps have timezone info (default to UTC if missing)
        # Prevents comparison errors between naive and timezone-aware datetimes
        if datetime_object.tzinfo is None:
            datetime_object = datetime_object.replace(tzinfo=datetime.timezone.utc)

        # Return timezone-aware datetime in UTC
        return datetime_object

    def _config_start_date(self):
        """Retrieve and parse the start_date from configuration.

        Returns the configured start_date as a datetime object for use in
        incremental sync. If no start_date is configured or if parsing fails,
        returns None to allow full sync from the beginning.

        Returns:
            datetime | None: Parsed start_date in UTC, or None if not configured/invalid
        """
        # Retrieve the start_date value from tap configuration (e.g., "2024-01-01")
        config_value = self.config.get("start_date")

        # If no start_date was configured, return None (no cutoff date)
        if not config_value:
            return None

        # Attempt to parse the configured date string into a datetime object
        try:
            return self._parse_iso8601(config_value)
        except Exception:
            # If parsing fails (invalid format), return None to avoid crashing
            # This allows the sync to proceed without a cutoff date
            return None

    def _effective_cutoff(self, context):
        """Determine the effective cutoff timestamp for incremental sync.

        Establishes the starting point for filtering records by checking two sources
        in priority order:
        1. State bookmark (timestamp from the last successful sync run)
        2. Configured start_date (from tap configuration)

        The state bookmark takes precedence to resume from where the previous sync
        left off. If no bookmark exists, falls back to the configured start_date.

        Args:
            context: Stream context (may contain parent stream information)

        Returns:
            datetime | None: The cutoff timestamp in UTC, or None if neither source is available
        """
        # Attempt to retrieve the state bookmark from the previous sync run
        # This represents the last_edited_time of the most recent record synced
        try:
            bookmark = self.get_starting_timestamp(context)  # type: ignore[attr-defined]
        except Exception:
            # If no state file exists or bookmark retrieval fails, set to None
            bookmark = None

        # Return the bookmark if available; otherwise fall back to configured start_date
        # This ensures incremental sync resumes from the correct point
        return bookmark or self._config_start_date()

    def get_next_page_token(self, response, previous_token):  # type: ignore[override]
        """Retrieve the next pagination token with early termination for incremental sync.

        Because results are sorted newest→oldest by last_edited_time, this method
        can short-circuit pagination once any record in the current page is older
        than the incremental cutoff timestamp. This optimization prevents unnecessary
        API calls when syncing recent changes.

        Args:
            response: The HTTP response object from the current request
            previous_token: The cursor token used for the current page (unused here)

        Returns:
            str | None: The next_cursor token for pagination, or None to stop
        """
        # Extract the next_cursor token from the response body
        # This token is used to fetch the next page of results
        next_cursor = (response.json() or {}).get("next_cursor")

        # Determine the effective cutoff timestamp for incremental sync
        cutoff_timestamp = self._effective_cutoff(context=None)

        # If no cutoff is set or no next page exists, return the token as-is
        # This allows full pagination when not doing incremental sync
        if not cutoff_timestamp or not next_cursor:
            return next_cursor

        # Extract the array of result records from the current page
        results = (response.json() or {}).get("results", [])

        # Track the oldest timestamp found in this page of results
        oldest_timestamp = None

        # Scan all records in the current page to find the oldest last_edited_time
        for record in results:
            # Get the last_edited_time field from the record
            timestamp_str = record.get("last_edited_time")
            if not timestamp_str:
                continue

            # Attempt to parse the timestamp string into a datetime object
            try:
                parsed_timestamp = self._parse_iso8601(timestamp_str)
            except Exception:
                # Skip records with unparseable timestamps
                continue

            # Update the oldest timestamp if this record is older
            if oldest_timestamp is None or parsed_timestamp < oldest_timestamp:
                oldest_timestamp = parsed_timestamp

        # If the oldest record in this page is older than the cutoff, stop paginating
        # All subsequent pages would contain even older records (due to descending sort)
        if oldest_timestamp is not None and oldest_timestamp < cutoff_timestamp:
            return None

        # Continue pagination: more recent records may still exist
        return next_cursor

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Filter records based on incremental cutoff timestamp.

        This method provides record-level filtering to exclude records older than
        the incremental cutoff. It acts as a safety net in addition to the
        pagination short-circuit in get_next_page_token.

        Args:
            row: The record dictionary to process
            context: Stream context (may contain parent stream information)

        Returns:
            dict | None: The processed record, or None to exclude it from output
        """
        # Apply any base transformations from the parent class
        row = super().post_process(row, context)

        # Determine the effective cutoff timestamp for incremental sync
        cutoff_timestamp = self._effective_cutoff(context)

        # If no cutoff is configured, keep all records (full sync mode)
        if not cutoff_timestamp:
            return row

        # Extract the last_edited_time field from the record
        timestamp_str = row.get("last_edited_time")

        # If a timestamp exists, check if the record is older than the cutoff
        if timestamp_str:
            try:
                # Parse the timestamp and compare with the cutoff
                parsed_timestamp = self._parse_iso8601(timestamp_str)
                if parsed_timestamp < cutoff_timestamp:
                    # Exclude records older than the cutoff by returning None
                    return None
            except Exception:
                # If timestamp parsing fails, keep the record rather than dropping it
                # This prevents data loss due to unexpected timestamp formats
                return row

        # Keep the record if it passes all filters
        return row

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Propagate page context to child streams for hierarchical data fetching.

        This method is called by the Singer SDK for each record retrieved by the
        search stream. When a record represents a page, it creates a context
        dictionary containing the page_id, which child streams (like PageDetailsStream
        or PageBlocksStream) will use to fetch related data.

        Only page objects trigger child streams. Databases and other object types
        are ignored to avoid unnecessary API calls.

        Args:
            record: The current record (page, database, etc.) from the search results
            context: Parent context (unused here, but available for nested hierarchies)

        Returns:
            dict | None: Context dict with page_id if record is a page, None otherwise
        """
        # Check if the current record represents a Notion page object
        # Other object types (database, block, etc.) will not trigger child streams
        if record.get("object") == "page":
            # Return a context dictionary containing the page ID
            # Child streams will receive this and use it to build their API requests
            return {"page_id": record["id"]}

        # Return None for non-page objects to skip child stream processing
        return None


class PagesIndexStream(NotionStream):
    """All accessible pages via search (POST /v1/search with object=page).

    Non-incremental index to ensure every accessible page is enumerated,
    independent of start_date or saved state. Used as parent for full page
    metadata and content streams.
    """

    # Stream identifier used in Singer tap operations and output
    name = "pages_index"
    # API endpoint path that will be combined with the base URL
    path = "/search"
    # HTTP method used for this endpoint (POST instead of GET)
    rest_method = "POST"
    # Field(s) that uniquely identify each record in the stream
    primary_keys: t.ClassVar[list[str]] = ["id"]
    # No incremental sync - always fetches all pages regardless of state
    replication_key = None

    # Reuse the same schema as SearchStream since both use the search endpoint
    schema = SearchStream.schema

    def prepare_request_payload(
            self,
            context: dict | None,
            next_page_token: t.Any | None,
    ) -> dict | None:
        """Build the POST request body for fetching all pages.

        Similar to SearchStream but without incremental filtering - this ensures
        a complete enumeration of all accessible pages.

        Args:
            context: Stream context (unused for this independent stream)
            next_page_token: Pagination cursor for fetching subsequent pages

        Returns:
            dict: The request body dictionary for the POST request
        """
        # Initialize empty dictionary to build the request body
        payload: dict[str, t.Any] = {}

        # Add pagination cursor to retrieve the next batch of results
        if next_page_token:
            payload["start_cursor"] = next_page_token

        # Always restrict results to pages only (exclude databases and other objects)
        # This is the key difference from SearchStream which allows flexible filtering
        payload["filter"] = {"property": "object", "value": "page"}

        # Set the number of results per request if configured
        # Uses config value or Notion's default if not specified
        page_size = self.config.get("page_size")
        if page_size:
            payload["page_size"] = page_size

        # Sort results by last modification time in descending order (newest first)
        # Maintains consistency with SearchStream ordering
        payload["sort"] = {"timestamp": "last_edited_time", "direction": "descending"}

        # Return the complete request body dictionary
        return payload

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Propagate page context to child streams for fetching detailed page data.

        Creates a context dictionary with page_id for each page record, enabling
        child streams to fetch page details, blocks, and other related data.

        Args:
            record: The current page record from the search results
            context: Parent context (unused here)

        Returns:
            dict | None: Context dict with page_id for page records
        """
        # Check if the record is a page object (should always be true for this stream)
        if record.get("object") == "page":
            # Return context dictionary containing the page ID for child streams
            return {"page_id": record["id"]}

        # Return None for non-page objects (shouldn't occur due to filter)
        return None


class PageDetailsStream(NotionStream):
    """Page metadata: GET /v1/pages/{page_id}.

    Fetches detailed metadata for individual pages including properties,
    parent information, and timestamps. This is a child stream that receives
    page_id values from its parent stream (SearchStream).
    """

    # Stream identifier used in Singer tap operations and output
    name = "pages"
    # API endpoint path with page_id placeholder that gets filled from context
    path = "/pages/{page_id}"
    # Parent stream that provides page_id values through context
    parent_stream_type = SearchStream
    # Field(s) that uniquely identify each record in the stream
    primary_keys: t.ClassVar[list[str]] = ["id"]

    def get_url_params(self, context, next_page_token):  # type: ignore[override]
        """Return empty query parameters for this endpoint.

        The Notion page details endpoint does not support pagination parameters.
        Unlike list endpoints, GET /v1/pages/{page_id} returns a single page object
        and will reject requests with `page_size` or `start_cursor` parameters.

        Args:
            context: Stream context containing page_id from parent
            next_page_token: Pagination cursor (unused for single-object endpoints)

        Returns:
            dict: Empty dictionary (no query parameters needed)
        """
        return {}

    def parse_response(self, response):  # type: ignore[override]
        """Parse the API response containing a single page object.

        Unlike paginated endpoints that return a `results` array, the page details
        endpoint returns a single page object directly. This method yields that
        object for processing by the Singer SDK.

        Args:
            response: The HTTP response object from the API request

        Yields:
            dict: The page object from the response body
        """
        # Yield the entire response body as a single record (no pagination)
        yield response.json()

    # Define the JSON schema for page metadata with all available fields
    schema = th.PropertiesList(
        # Type of Notion object (always "page" for this stream)
        th.Property("object", th.StringType),
        # Unique identifier for the page
        th.Property("id", th.StringType),
        # Timestamp when the page was created
        th.Property("created_time", th.DateTimeType),
        # Timestamp when the page was last modified
        th.Property("last_edited_time", th.DateTimeType),
        # Whether the page has been archived/deleted
        th.Property("archived", th.BooleanType),
        # Parent object information (workspace, database, or another page)
        th.Property("parent", th.ObjectType()),
        # Custom properties defined for the page (structure varies by parent)
        th.Property("properties", th.ObjectType()),
        # URL to access the page in Notion
        th.Property("url", th.StringType)
    ).to_dict()


class PageBlocksStream(NotionStream):
    """Top-level blocks for each page: GET /v1/blocks/{page_id}/children

    Fetches the immediate child blocks of a page without recursive traversal.
    For full recursive traversal of all nested blocks, use BlockChildrenStream instead.
    This stream is a child of SearchStream and receives page_id values from it.
    """

    # Stream identifier used in Singer tap operations and output
    name = "page_blocks"
    # API endpoint path with page_id placeholder filled from context
    path = "/blocks/{page_id}/children"
    # Parent stream that provides page_id values through context
    parent_stream_type = SearchStream
    # Field(s) that uniquely identify each record in the stream
    primary_keys: t.ClassVar[list[str]] = ["id"]

    # Define the JSON schema for block objects with all available fields
    schema = th.PropertiesList(
        # Type of Notion object (always "block" for this stream)
        th.Property("object", th.StringType),
        # Unique identifier for the block
        th.Property("id", th.StringType),
        # Block type (paragraph, heading_1, image, etc.)
        th.Property("type", th.StringType),
        # Whether this block contains nested child blocks
        th.Property("has_children", th.BooleanType),
        # Whether the block has been archived/deleted
        th.Property("archived", th.BooleanType),
        # Timestamp when the block was created
        th.Property("created_time", th.DateTimeType),
        # Timestamp when the block was last modified
        th.Property("last_edited_time", th.DateTimeType),
        # Parent object information (page or another block)
        th.Property("parent", th.ObjectType()),
        # Block-specific payloads (generic object types)
        # Each property corresponds to a specific block type and contains type-specific data
        th.Property("paragraph", th.ObjectType()),
        th.Property("heading_1", th.ObjectType()),
        th.Property("heading_2", th.ObjectType()),
        th.Property("heading_3", th.ObjectType()),
        th.Property("bulleted_list_item", th.ObjectType()),
        th.Property("numbered_list_item", th.ObjectType()),
        th.Property("to_do", th.ObjectType()),
        th.Property("toggle", th.ObjectType()),
        th.Property("quote", th.ObjectType()),
        th.Property("callout", th.ObjectType()),
        th.Property("code", th.ObjectType()),
        th.Property("image", th.ObjectType()),
        th.Property("video", th.ObjectType()),
        th.Property("file", th.ObjectType()),
        th.Property("pdf", th.ObjectType()),
        th.Property("bookmark", th.ObjectType()),
        th.Property("embed", th.ObjectType()),
        th.Property("equation", th.ObjectType()),
        th.Property("synced_block", th.ObjectType()),
        th.Property("template", th.ObjectType()),
        th.Property("child_page", th.ObjectType()),
        th.Property("child_database", th.ObjectType()),
        th.Property("link_to_page", th.ObjectType()),
        # Enrichment fields added by this stream (not from Notion API)
        # Custom field to track which page this block belongs to
        th.Property("_page_id", th.StringType),
        # Custom field to track the parent block ID for nested blocks
        th.Property("_parent_block_id", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:  # type: ignore[override]
        """Enrich block records with lineage information.

        Adds the originating page_id to each block record to maintain the relationship
        between blocks and their parent page. This is useful for downstream processing
        where you need to know which page a block came from.

        Args:
            row: The block record to process
            context: Stream context containing page_id from parent stream

        Returns:
            dict | None: The enriched block record with _page_id field
        """
        # Apply any base transformations from the parent class
        row = super().post_process(row, context)

        # Add the page_id to the record if available in context
        # This enriches the block with information about which page it belongs to
        if context and "page_id" in context:
            row["_page_id"] = context["page_id"]

        # Return the enriched record
        return row

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Emit child context for blocks that have nested children.

        This enables BlockChildrenStream (or other child block streams) to
        continue traversal from the current block. We also propagate the
        originating `page_id` for lineage tracking across the block hierarchy.

        Args:
            record: The current block record
            context: Parent context containing page_id

        Returns:
            dict | None: Context dict with block_id and page_id if block has children,
                        None otherwise to skip child stream processing
        """
        # Check if this block contains nested children that need to be fetched
        if record.get("has_children"):
            # Return context for child streams to fetch nested blocks
            return {
                # ID of the current block (becomes parent for next level)
                "block_id": record.get("id"),
                # Propagate the originating page_id for complete lineage tracking
                "page_id": context.get("page_id") if context else None,
            }

        # Return None for leaf blocks (no children) to skip child stream processing
        return None


class BlockChildrenStream(NotionStream):
    """Recursively fetch all blocks for a page, including nested children.

    This stream performs a complete depth-first traversal of the block tree starting
    from a page_id and emits every block with enriched lineage fields (_page_id and
    _parent_block_id). Unlike PageBlocksStream which only fetches immediate children,
    this stream recursively descends into all nested blocks.

    Note: Referenced pages (link_to_page blocks) are not followed to avoid circular
    dependencies and unbounded traversal.

    Parent stream: PagesIndexStream (receives page_id through context)
    """

    # Stream identifier used in Singer tap operations and output
    name = "block_children"
    # Path template (unused - custom get_records implementation bypasses the base URL builder)
    path = "/blocks/{block_id}/children"
    # Parent stream that provides page_id values through context
    parent_stream_type = PagesIndexStream
    # Field(s) that uniquely identify each record in the stream
    primary_keys: t.ClassVar[list[str]] = ["id"]

    # Reuse the schema from PageBlocksStream since both emit block objects
    schema = PageBlocksStream.schema

    def get_records(self, context: dict | None) -> t.Iterable[dict]:  # type: ignore[override]
        """Entry point for record generation called by the Singer SDK.

        This stream expects a `page_id` in the context (provided by the parent
        PagesIndexStream) and will recursively traverse all blocks for that page,
        yielding each block with enriched lineage fields.

        Args:
            context: Stream context dictionary containing page_id from parent stream

        Yields:
            dict: Block records with _page_id and _parent_block_id enrichment fields
        """
        # Validate that we have the required page_id context from parent stream
        if not context or "page_id" not in context:
            return []

        # Extract the page ID that we'll start traversing from
        page_id = context["page_id"]

        # Begin depth-first traversal of all blocks in the page
        yield from self._walk_blocks(page_id=page_id)

    def _walk_blocks(self, page_id: str) -> t.Iterable[dict]:
        """Initiate depth-first traversal of all blocks for the given page.

        This is the entry point for block tree traversal. It starts by fetching
        the page's top-level children and then recursively descends into nested
        blocks as indicated by the has_children flag.

        Args:
            page_id: The Notion page ID to start traversal from

        Yields:
            dict: Block records from the entire block tree
        """
        # Start with the page's immediate children blocks (parent_block_id is None for top-level)
        yield from self._iter_children(
            parent_id=page_id,
            page_id=page_id,
            parent_block_id=None
        )

    def _iter_children(
            self,
            parent_id: str,
            page_id: str,
            parent_block_id: str | None,
    ) -> t.Iterable[dict]:
        """Fetch and yield all children of a parent block or page, with recursive descent.

        This method handles:
        1. Paginated fetching of blocks from Notion API
        2. Enriching each block with lineage metadata (_page_id, _parent_block_id)
        3. Recursive traversal into blocks that have nested children

        Args:
            parent_id: The ID of the parent block (or page_id for top-level blocks)
            page_id: The originating page ID (propagated for lineage tracking)
            parent_block_id: The block ID of the immediate parent (None for page-level blocks)

        Yields:
            dict: Block records with enriched lineage fields, including all nested descendants
        """
        # Build authentication headers by copying base headers and adding auth token
        request_headers = {**self.http_headers}
        auth_token = self.config.get("auth_token")
        if auth_token:
            request_headers["Authorization"] = f"Bearer {auth_token}"

        # Initialize query parameters dictionary for pagination and page size control
        query_params: dict[str, t.Any] = {}

        # Add page_size parameter if configured (controls results per request)
        configured_page_size = self.config.get("page_size")
        if configured_page_size:
            query_params["page_size"] = configured_page_size

        # Track pagination cursor for fetching subsequent pages of results
        pagination_cursor: str | None = None

        # Paginate through all children of the current parent block/page
        while True:
            # Construct the API endpoint URL for fetching children of the parent
            api_url = f"{self.url_base}/blocks/{parent_id}/children"

            # Add pagination cursor to query params if we're fetching a subsequent page
            if pagination_cursor:
                query_params["start_cursor"] = pagination_cursor

            # Make HTTP GET request to Notion API with authentication and pagination
            response = requests.get(
                api_url,
                headers=request_headers,
                params=query_params,
                timeout=60
            )
            # Raise exception if request failed (4xx or 5xx status codes)
            response.raise_for_status()

            # Parse JSON response body into dictionary
            response_data = response.json()

            # Extract the array of block objects from the results field
            blocks = response_data.get("results", [])

            # Process each block in the current page of results
            for block in blocks:
                # Enrich block with originating page ID for lineage tracking
                # Use setdefault to avoid overwriting if field already exists
                block.setdefault("_page_id", page_id)

                # Enrich block with immediate parent block ID (if not a top-level block)
                if parent_block_id:
                    block.setdefault("_parent_block_id", parent_block_id)

                # Yield the enriched block record to the caller
                yield block

                # Recursively descend into nested children if this block contains them
                if block.get("has_children"):
                    # Extract the block ID to use as parent for the next level
                    current_block_id = block.get("id")

                    # Validate that block ID exists and is a string before recursing
                    if isinstance(current_block_id, str):
                        # Recursive call to fetch all descendants of this block
                        yield from self._iter_children(
                            parent_id=current_block_id,
                            page_id=page_id,  # Propagate original page_id
                            parent_block_id=current_block_id,  # This block becomes parent
                        )

            # Check if there are more pages of results to fetch
            pagination_cursor = response_data.get("next_cursor")

            # Exit pagination loop if no more pages exist (next_cursor is None/empty)
            if not pagination_cursor:
                break