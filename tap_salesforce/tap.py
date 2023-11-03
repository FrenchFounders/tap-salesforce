"""salesforce tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_salesforce import streams


class Tapsalesforce(Tap):
    """salesforce tap class."""

    name = "tap-salesforce"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="The username (or email address) used to sign in to your Salesforce account.",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            secret=True,
            description="The password used to sign in to your Salesforce account.",
        ),
        th.Property(
            "security_token",
            th.StringType,
            required=True,
            secret=True,
            description="The security token used to sign in to your Salesforce account.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.salesforceStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ContentDocumentLinksStream(self),
            streams.ContentNotesStream(self),
        ]

if __name__ == "__main__":
    Tapsalesforce.cli()
