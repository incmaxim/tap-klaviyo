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

    def setup_mapper(self):
        """Set up the stream mapper."""
        self._config.setdefault("flattening_enabled", True)
        self._config.setdefault("flattening_max_depth", 2)
        return super().setup_mapper()

    def discover_streams(self) -> List[streams.KlaviyoStream]:
        """Return a list of discovered streams."""
        enabled_streams = []
        
        for stream_type in STREAM_TYPES:
            stream_name = stream_type.__name__.lower().replace('stream', '')
            if self.config.get(f"enable_{stream_name}", True):
                enabled_streams.append(stream_type)
                
        return [stream_class(tap=self) for stream_class in enabled_streams]


if __name__ == "__main__":
    TapKlaviyo.cli()
