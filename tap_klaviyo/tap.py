"""Klaviyo tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

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
            "refresh_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The refresh token to authenticate against the API service",
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            secret=True,
            description="The client ID for OAuth authentication",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            secret=True,
            description="The client secret for OAuth authentication",
        ),
        th.Property(
            "revision",
            th.StringType,
            required=True,
            description="Klaviyo API endpoint revision. https://developers.klaviyo.com/en/docs/api_versioning_and_deprecation_policy#versioning",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "selected_streams",
            th.ArrayType(th.StringType),
            required=False,
            description="List of streams to sync. If not provided, all streams will be synced.",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.KlaviyoStream]:
        """Return a list of discovered streams."""
        available_streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]
        
        # Dodaj events stream samo ako je eksplicitno zatražen
        if self.config.get("enable_events_stream", False):
            available_streams.append(streams.EventsStream(tap=self))
        
        # Filtriraj streamove ako je specificirana selekcija
        if selected_streams := self.config.get('selected_streams'):
            self.logger.info(f"Filtering streams based on selection: {selected_streams}")
            return [
                stream for stream in available_streams 
                if stream.name in selected_streams
            ]
        
        self.logger.info("No stream selection provided, returning all streams")
        return available_streams


if __name__ == "__main__":
    TapKlaviyo.cli()
