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
    """Workspace search stream (POST /v1/search)."""

    name = "search"
    path = "/search"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
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
        return payload or {}
