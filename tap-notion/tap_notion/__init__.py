"""tap_notion package for the Notion Singer tap.

Contents:
- tap.py: Tap entrypoint (configuration and stream discovery).
- client.py: NotionStream base class (auth, headers, pagination, parsing).
- streams.py: Concrete streams implementing Notion API endpoints and
  parent/child relationships.

If you're new to Meltano/Singer, read ARCHITECTURE.md in the repo root for a
step-by-step explanation of how these pieces fit together.
"""
