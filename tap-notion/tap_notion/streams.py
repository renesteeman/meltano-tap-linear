"""Stream type classes for tap-notion."""

from __future__ import annotations

import typing as t
from importlib import resources

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_notion.client import NotionStream

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
