# How this tap works (tap-notion)

This guide explains the architecture of the Notion Singer tap built with the Meltano Singer SDK. It is intended for newcomers who want to understand how the code is structured, what functionality is available, and how to extend or use it safely.

- Project: tap-notion
- Runtime: Singer SDK (via Meltano or standalone)
- Source API: Notion public API v1

## High-level architecture

The tap is composed of three conceptual layers:

1. Tap (tap_notion/tap.py)
   - Declares the tap name, configuration schema, and the list of streams the tap exposes.
   - The SDK uses this to provide `--about`, `--discover`, and the standard Singer CLI behavior.

2. Base stream (tap_notion/client.py)
   - `NotionStream` extends the SDK's `RESTStream` to centralize Notion-specific defaults:
     - Base URL: `https://api.notion.com/v1`
     - Authentication: Bearer token via `auth_token`.
     - Headers: `Notion-Version` (configurable) and optional `User-Agent`.
     - Response parsing: Uses JSONPath to extract results from the standard Notion envelope `{ results: [...], next_cursor: ... }`.
     - URL params: Applies `page_size` and `start_cursor` automatically for GET endpoints.

3. Concrete streams (tap_notion/streams.py)
   - Implement API endpoints and schemas and, where needed, custom payloads or traversal logic. The current streams are:
     - `UsersStream` (GET /v1/users) — lists users.
     - `SearchStream` (POST /v1/search) — lists pages and databases using Notion search. Implements incremental sync using `last_edited_time` with a client-side cutoff.
     - `PagesIndexStream` (POST /v1/search with `object=page`) — a non-incremental index of pages used to drive child page streams.
     - `PageDetailsStream` (GET /v1/pages/{page_id}) — fetches the full page metadata for each page.
     - `PageBlocksStream` (GET /v1/blocks/{page_id}/children) — lists top-level blocks on each page.
     - `BlockChildrenStream` — recursively walks the full block tree for each page by following `has_children` pointers.

## Stream relationships, context, and dependencies

- The tap defines a set of streams and their parent/child relationships using `parent_stream_type` and `get_child_context`.
- `SearchStream` can act as a parent for page-based child streams by emitting a context with `{ "page_id": ... }` only for `object == "page"` results.
- `PagesIndexStream` is a non-incremental parent that guarantees coverage of all accessible pages and is used by `BlockChildrenStream` for comprehensive traversal.
- Child streams retrieve their `page_id` (or `block_id`) from the context to form endpoint URLs like `/pages/{page_id}` or `/blocks/{page_id}/children`.

## Incremental sync strategy

- `SearchStream`:
  - Sets `replication_key = "last_edited_time"` and sorts results by `last_edited_time` descending in the request payload.
  - Uses `start_date` from config on first run; on subsequent runs, the Singer state bookmark supersedes the config value.
  - Applies a client-side cutoff in `post_process` to drop rows older than the effective cutoff and short-circuits pagination in `get_next_page_token` once a page contains results older than the cutoff.

- Other streams:
  - `PagesIndexStream` is non-incremental by design to enumerate every accessible page. Use sparingly if you want to limit scope.
  - `PageDetailsStream` and `PageBlocksStream` inherit pagination defaults and rely on contexts emitted by a parent stream.
  - `BlockChildrenStream` implements a custom recursive traversal with direct HTTP calls because the Notion blocks API requires following child links; it enriches emitted rows with lineage fields: `_page_id` and `_parent_block_id`.

## Configuration and how it is used

- `auth_token` (required): Notion integration token used for Bearer authentication.
- `start_date` (optional): Initial cutoff for incremental sync on `SearchStream`. Accepts ISO8601/RFC3339 timestamps (e.g., `2024-01-01T00:00:00Z`) and date-only strings (e.g., `2024-01-01`, interpreted as midnight UTC). On subsequent runs, the state bookmark is used instead.
- `notion_version` (optional): Overrides the `Notion-Version` header; defaults to `2022-06-28`.
- `page_size` (optional): Controls page size for list/search endpoints. Notion maximum is 100.
- `user_agent` (optional): Custom `User-Agent` header value.
- `search_filter_object` (optional): Adds a basic search filter on `SearchStream`: `page` or `database`.
- `search_query` (optional): Adds a query string to `SearchStream` requests.

The SDK reads these from the JSON config, environment variables (if using `--config=ENV`), or Meltano config and passes them to stream instances via `self.config`.

## Pagination and envelopes

- Notion list/search endpoints return an envelope with `results` and `next_cursor`.
- `NotionStream` sets `records_jsonpath = "$.results[*]"` and `next_page_token_jsonpath = "$.next_cursor"` and defers pagination to the SDK unless custom behavior is required.
- For GET endpoints, URL params `start_cursor` and `page_size` are set automatically. For POST endpoints, streams set these in the JSON payload.

## Running the tap

- Discover:
  - `tap-notion --config config.json --discover > catalog.json`
- Select streams in `catalog.json` (or use Meltano selections) and run the tap.

Minimal example `config.json`:

```json
{
  "auth_token": "secret_xxx",
  "start_date": "2024-01-01T00:00:00Z",
  "page_size": 100
}
```

## Extending the tap

- Add a new stream by subclassing `NotionStream` and set:
  - `name`, `path`, `primary_keys`, `schema`, and optionally `rest_method`.
  - If POST, implement `prepare_request_payload` to include `start_cursor`, `page_size`, and any filters.
  - Override `get_url_params` or `parse_response` if the endpoint deviates from the standard envelope.
  - If the stream depends on parent records, set `parent_stream_type` and implement `get_child_context` on the parent.

- Register your stream in `TapNotion.discover_streams`.

## Known limitations and notes

- The Notion API paginates at 100 items maximum; extraction of large workspaces can take time.
- `BlockChildrenStream` does not follow `link_to_page` references to other pages; it only traverses within the starting page's block tree.
- Some schemas use generic `ObjectType` because Notion block properties vary by block `type`.
- Rate limits: The SDK will retry on common errors, but large jobs should consider backoff and Meltano orchestration.

## Where to look in the code

- Tap definition: `tap_notion/tap.py`
- Base stream and API mechanics: `tap_notion/client.py`
- Concrete streams and traversal logic: `tap_notion/streams.py`
- Basic tests: `tests/test_core.py`

## Further reading

- Singer SDK Docs: https://sdk.meltano.com/
- Notion API Docs: https://developers.notion.com/reference
