# tap-notion

`tap-notion` is a Singer tap for Notion.

Built with the Meltano Tap SDK for Singer Taps.

## Installation

Install from GitHub by adding

  - name: tap-notion
    namespace: tap_notion
    pip_url: git+https://github.com/renesteeman/meltano-tap-notion@main#subdirectory=tap-notion
    executable: tap-notion
    capabilities:
      - discover
      - catalog
      - state
    settings:
      - name: auth_token
        kind: password
        sensitive: true

to meltano.yml

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this
tap is available by running:

```
tap-notion --about
```

Key settings used by this tap:
- auth_token (required)
  - Notion integration token used for Bearer auth.
- notion_version (optional)
  - Overrides the Notion-Version header. Defaults to 2022-06-28.
- page_size (optional)
  - Controls page size for /users (GET) and /search (POST). Max 100.
- user_agent (optional)
  - Custom User-Agent header value.
- search_filter_object (optional)
  - Adds a simple object filter to /search: "page" or "database".
- search_query (optional)
  - Adds a query string to /search requests.
- start_date (optional)
  - Initial cutoff for incremental sync on the search stream only. The tap sorts search results by last_edited_time (newest first) and filters client-side to drop rows older than this timestamp. On subsequent runs, Singer state supersedes start_date.

Accepted formats for start_date include ISO8601/RFC3339 timestamps like "2024-01-01T00:00:00Z" and date-only strings like "2024-01-01" (treated as midnight UTC).

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

## Usage

You can easily run `tap-notion` by itself or in a pipeline using Meltano.

### Executing the Tap Directly

```
tap-notion --version
tap-notion --help
tap-notion --config CONFIG --discover > ./catalog.json
```

To run just the search stream incrementally with a start_date:

```
tap-notion \
  --config config.json \
  --catalog <(tap-notion --config config.json --discover | jq '.streams |= map(select(.tap_stream_id == "search"))')
```

Where config.json contains:

```
{
  "auth_token": "secret_xxx",
  "start_date": "2024-01-01T00:00:00Z"
}
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.10+
- uv

```
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```
uv run pytest
```

You can also test the `tap-notion` CLI interface directly using `uv run`:

```
uv run tap-notion --help
```

### SDK Dev Guide

See the dev guide for more instructions on how to use the SDK to
develop your own taps and targets.
