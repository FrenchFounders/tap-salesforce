"""Stream type classes for tap-salesforce."""

from __future__ import annotations

import types
import typing as t
from pathlib import Path
from datetime import datetime, timedelta

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_salesforce.client import SalesforceStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class ContentDocumentLinksStream(SalesforceStream):
    """ContentDocumentLinks stream"""

    def __init__(self, tap, source):
        self.source = source
        super().__init__(tap)

    path = "/services/data/v59.0/queryAll"
    primary_keys: t.ClassVar[list[str]] = ["Id"]
    replication_key = "SystemModstamp"
    records_jsonpath = "$.records[*]"
    next_page_token_jsonpath = "$.nextRecordsUrl"
    schema_filepath = SCHEMAS_DIR / "content_document_links.json"

    @property
    def name(self):
        return "content_document_links_{}".format(self.source)

    def get_url_params(
            self, context: th.Optional[dict], next_page_token: th.Optional[th.Any]
    ) -> th.Dict[str, th.Any]:
        params = super().get_url_params(context, next_page_token)
        params["q"] = f"SELECT Id,LinkedEntityId,ContentDocumentId,IsDeleted,SystemModstamp FROM ContentDocumentLink WHERE LinkedEntityId IN (SELECT Id FROM {self.source})"
        replication_key = self.get_starting_replication_key_value(context)
        if replication_key is not None:
            # SystemModstamp is not updated when the content is updated, remove 2 weeks to track modifications
            replication_date = datetime.strptime(replication_key, '%Y-%m-%dT%H:%M:%S.%f%z') - timedelta(weeks=2)
            params["q"] += f" AND SystemModstamp > {replication_date.strftime('%Y-%m-%dT%H:%M:%SZ')}"

        params["q"] += " ORDER BY SystemModstamp LIMIT 500"

        return params

    def prepare_request(self, context: dict | None, next_page_token=None):
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict | str = self.get_url_params(context, next_page_token)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers
        if next_page_token is not None:
            url = self._instance_url + next_page_token

        return self.build_prepared_request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )

    def get_child_context(self, record: dict, context: t.Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"ContentDocumentId": record["ContentDocumentId"]}


class ContentNoteContentsStream(SalesforceStream):
    """
    ContentNoteContents stream
    """

    def __init__(self, tap):
        super().__init__(tap)

    parent_stream_type = ContentDocumentLinksStream
    name = 'content_note_contents'
    primary_keys: t.ClassVar[list[str]] = ["ContentDocumentId"]
    ignore_parent_replication_keys = False
    schema_filepath = SCHEMAS_DIR / "content_note_contents.json"
    replication_key = None
    state_partitioning_keys = []

    @property
    def path(self):
        return "/services/data/v59.0/sobjects/ContentNote/{ContentDocumentId}/Content"

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        content = None
        if response.status_code == requests.codes.ok:
            content = response.text

        return [{
            "Content": content
        }]

    def post_process(self, row: types.Record, context: types.Context | None = None) -> dict | None:
        if row["Content"] is not None:
            row["ContentDocumentId"] = context.get("ContentDocumentId")
            return row


class ContentNotesStream(SalesforceStream):
    """
    ContentNotes stream
    """

    def __init__(self, tap):
        super().__init__(tap)

    parent_stream_type = ContentDocumentLinksStream
    name = 'content_notes'
    primary_keys: t.ClassVar[list[str]] = ["Id"]
    ignore_parent_replication_keys = False
    schema_filepath = SCHEMAS_DIR / "content_notes.json"
    replication_key = None
    state_partitioning_keys = []

    @property
    def path(self):
        return "/services/data/v59.0/sobjects/ContentNote/{ContentDocumentId}"

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        if response.status_code == requests.codes.ok:
            yield from super().parse_response(response)
