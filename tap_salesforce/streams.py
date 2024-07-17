"""Stream type classes for tap-salesforce."""

from __future__ import annotations

import typing as t
from pathlib import Path
from datetime import datetime, timedelta

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
        """Set the path for the stream."""
        return "/services/data/v59.0/sobjects/ContentNote/{ContentDocumentId}/Content"

    def parse_response(self, response):
        response_dict = {
            'Content': response.text
        }
        return [response_dict]

    def get_records(self, context: t.Optional[dict]):
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        for record in self.request_records(context):
            try:
                transformed_record = self.post_process(record, context)
                transformed_record['ContentDocumentId'] = context.get("ContentDocumentId")
            except:
                transformed_record = {'Content': None, 'ContentDocumentId': context.get("ContentDocumentId")}
            if transformed_record is None or ('resource does not exist' in transformed_record['Content']):
                # Record filtered out during post_process()
                continue
            yield transformed_record


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
        """Set the path for the stream."""
        return "/services/data/v59.0/sobjects/ContentNote/{ContentDocumentId}"
