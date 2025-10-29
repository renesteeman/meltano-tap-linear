"""Stream type classes for tap-notion."""

from __future__ import annotations

import typing as t
from importlib import resources

from singer_sdk import typing as th  # JSON Schema typing helpers

from .client import NotionStream

# JSON schemas directory (unused currently but kept for future use)
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class UsersStream(NotionStream):
    """Notion users stream (GET /v1/users)."""

    name = "users"
    path = "/users"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("object", th.StringType),
        th.Property("id", th.StringType, description="User ID"),
        th.Property("type", th.StringType),
        th.Property("name", th.StringType),
        th.Property("avatar_url", th.StringType),
        th.Property(
            "person",
            th.ObjectType(
                th.Property("email", th.StringType),
            ),
        ),
        th.Property(
            "bot",
            th.ObjectType(
                th.Property("owner", th.ObjectType()),
            ),
        ),
    ).to_dict()


class SearchStream(NotionStream):
    """Workspace search stream (POST /v1/search).

    Implements incremental sync using last_edited_time and respects
    config start_date or stored state as the cutoff.
    """

    name = "search"
    path = "/search"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "last_edited_time"
    rest_method = "POST"

    # Search returns same envelope with `results` and `next_cursor`.
    schema = th.PropertiesList(
        th.Property("object", th.StringType),
        th.Property("id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_edited_time", th.DateTimeType),
        th.Property("archived", th.BooleanType),
        th.Property("icon", th.ObjectType()),
        th.Property("cover", th.ObjectType()),
        th.Property("properties", th.ObjectType()),
        th.Property("parent", th.ObjectType()),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict | None:
        payload: dict[str, t.Any] = {}
        if next_page_token:
            payload["start_cursor"] = next_page_token
        # Allow optional simple filter via config
        filter_object = self.config.get("search_filter_object")  # e.g. "page" or "database"
        if filter_object:
            payload["filter"] = {"property": "object", "value": filter_object}
        # Optional query string
        query = self.config.get("search_query")
        if query:
            payload["query"] = query
        page_size = self.config.get("page_size")
        if page_size:
            payload["page_size"] = page_size
        # Ensure newest-first ordering to align with incremental cutoff logic
        payload["sort"] = {"timestamp": "last_edited_time", "direction": "descending"}
        return payload or {}

    # --- Incremental filtering helpers ---
    @staticmethod
    def _parse_iso8601(ts: str):
        import datetime as _dt

        s = ts.strip()
        # Handle date-only strings like 'YYYY-MM-DD'
        if len(s) == 10 and "T" not in s:
            d = _dt.date.fromisoformat(s)
            return _dt.datetime(d.year, d.month, d.day, tzinfo=_dt.timezone.utc)
        # Replace trailing 'Z' with explicit UTC offset for fromisoformat
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = _dt.datetime.fromisoformat(s)
        # If no timezone info, assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        return dt

    def _config_start_date(self):
        cfg = self.config.get("start_date")
        if not cfg:
            return None
        try:
            return self._parse_iso8601(cfg)
        except Exception:
            return None

    def _effective_cutoff(self, context):
        # Prefer state bookmark when available; otherwise use config start_date
        try:
            bookmark = self.get_starting_timestamp(context)  # type: ignore[attr-defined]
        except Exception:
            bookmark = None
        return bookmark or self._config_start_date()

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        # Apply base transformations if any
        row = super().post_process(row, context)
        cutoff = self._effective_cutoff(context)
        if not cutoff:
            return row
        ts = row.get("last_edited_time")
        if ts:
            try:
                if self._parse_iso8601(ts) < cutoff:
                    return None
            except Exception:
                # If timestamp unparsable, keep the row rather than dropping silently
                return row
        return row

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Propagate page contexts to child streams.

        Only create child contexts for page objects. Databases and other objects
        will be ignored by page child streams.
        """
        if record.get("object") == "page":
            return {"page_id": record["id"]}
        return None



class PagesIndexStream(NotionStream):
    """All accessible pages via search (POST /v1/search with object=page).

    Non-incremental index to ensure every accessible page is enumerated,
    independent of start_date or saved state. Used as parent for full page
    metadata and content streams.
    """

    name = "pages_index"
    path = "/search"
    rest_method = "POST"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None

    schema = SearchStream.schema

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict | None:
        payload: dict[str, t.Any] = {}
        if next_page_token:
            payload["start_cursor"] = next_page_token
        # Always restrict to pages only
        payload["filter"] = {"property": "object", "value": "page"}
        page_size = self.config.get("page_size")
        if page_size:
            payload["page_size"] = page_size
        payload["sort"] = {"timestamp": "last_edited_time", "direction": "descending"}
        return payload

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        if record.get("object") == "page":
            return {"page_id": record["id"]}
        return None


class PageDetailsStream(NotionStream):
    """Page metadata: GET /v1/pages/{page_id}."""

    name = "pages"
    path = "/pages/{page_id}"
    parent_stream_type = PagesIndexStream
    primary_keys: t.ClassVar[list[str]] = ["id"]

    def parse_response(self, response):  # type: ignore[override]
        yield response.json()

    schema = th.PropertiesList(
        th.Property("object", th.StringType),
        th.Property("id", th.StringType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_edited_time", th.DateTimeType),
        th.Property("archived", th.BooleanType),
        th.Property("parent", th.ObjectType()),
        th.Property("properties", th.ObjectType()),
        th.Property("url", th.StringType),
        th.Property("icon", th.ObjectType()),
        th.Property("cover", th.ObjectType()),
    ).to_dict()


class PageBlocksStream(NotionStream):
    """Top-level blocks for each page: GET /v1/blocks/{page_id}/children"""

    name = "page_blocks"
    path = "/blocks/{page_id}/children"
    parent_stream_type = PagesIndexStream
    primary_keys: t.ClassVar[list[str]] = ["id"]

    schema = th.PropertiesList(
        th.Property("object", th.StringType),
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("has_children", th.BooleanType),
        th.Property("archived", th.BooleanType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_edited_time", th.DateTimeType),
        th.Property("parent", th.ObjectType()),
        # Block-specific payloads (generic object types)
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
        # Enrichment fields
        th.Property("_page_id", th.StringType),
        th.Property("_parent_block_id", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:  # type: ignore[override]
        row = super().post_process(row, context)
        if context and "page_id" in context:
            row["_page_id"] = context["page_id"]
        return row

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        if record.get("has_children"):
            return {"block_id": record.get("id"), "page_id": context.get("page_id") if context else None}
        return None


class BlockChildrenStream(NotionStream):
    """All blocks for a page, recursively.

    Recursively traverses the block tree starting at a page_id and emits every
    block with lineage fields. Referenced pages (link_to_page) are not followed.
    """

    name = "block_children"
    # Path is unused by the custom get_records implementation
    path = "/blocks/{block_id}/children"
    parent_stream_type = PagesIndexStream
    primary_keys: t.ClassVar[list[str]] = ["id"]

    schema = PageBlocksStream.schema

    def get_records(self, context: dict | None) -> t.Iterable[dict]:  # type: ignore[override]
        if not context or "page_id" not in context:
            return []
        page_id = context["page_id"]
        yield from self._walk_blocks(page_id=page_id)

    def _walk_blocks(self, page_id: str) -> t.Iterable[dict]:
        """Depth-first traversal yielding all blocks for the given page.

        Args:
            page_id: The Notion page ID to start from.
        """
        # Start with the page's top-level children
        yield from self._iter_children(parent_id=page_id, page_id=page_id, parent_block_id=None)

    def _iter_children(
        self,
        parent_id: str,
        page_id: str,
        parent_block_id: str | None,
    ) -> t.Iterable[dict]:
        import requests

        headers = {**self.http_headers}
        token = self.config.get("auth_token")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        params: dict[str, t.Any] = {}
        page_size = self.config.get("page_size")
        if page_size:
            params["page_size"] = page_size
        next_cursor: str | None = None
        while True:
            url = f"{self.url_base}/blocks/{parent_id}/children"
            if next_cursor:
                params["start_cursor"] = next_cursor
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            results = payload.get("results", [])
            for block in results:
                # Enrich lineage
                block.setdefault("_page_id", page_id)
                if parent_block_id:
                    block.setdefault("_parent_block_id", parent_block_id)
                yield block
                # Recurse into children if indicated
                if block.get("has_children"):
                    child_id = block.get("id")
                    if isinstance(child_id, str):
                        yield from self._iter_children(
                            parent_id=child_id,
                            page_id=page_id,
                            parent_block_id=child_id,
                        )
            next_cursor = payload.get("next_cursor")
            if not next_cursor:
                break
