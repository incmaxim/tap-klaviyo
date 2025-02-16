"""Klaviyo tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from typing import List

from tap_klaviyo import streams

STREAM_TYPES = [
    streams.CampaignsStream,
    streams.MetricsStream,
    streams.ProfilesStream,
    streams.ListsStream,
    streams.ListPersonStream,
    streams.FlowsStream,
    streams.TemplatesStream,
]

class TapKlaviyo(Tap):
    """Klaviyo tap class."""

    name = "tap-klaviyo"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="The OAuth client ID",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            secret=True,
            description="The OAuth client secret",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
            secret=True,
            description="The OAuth refresh token",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> List[streams.KlaviyoStream]:
        """Return a list of discovered streams."""
        # Inicijaliziramo sve streamove
        streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]
        # Vraćamo samo one koji su selektovani kroz Meltano konfiguraciju
        return [stream for stream in streams if stream.selected]


if __name__ == "__main__":
    TapKlaviyo.cli()
