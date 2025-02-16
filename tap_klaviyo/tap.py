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
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> List[streams.KlaviyoStream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapKlaviyo.cli()
