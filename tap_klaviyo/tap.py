"""Klaviyo tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_klaviyo import streams


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
    ).to_dict()

    def discover_streams(self) -> list[streams.KlaviyoStream]:
        """Return a list of discovered streams."""
        available_streams = [
            streams.EventsStream(self),
            streams.CampaignsStream(self),
            streams.MetricsStream(self),
            streams.ProfilesStream(self),
            streams.ListsStream(self),
            streams.ListPersonStream(self),
            streams.FlowsStream(self),
            streams.TemplatesStream(self),
        ]
        
        # Dobavi imena selektiranih i isključenih streamova iz konfiguracije
        selected = set()
        excluded = set()
        
        for selection in self.config.get('select', []):
            if selection.startswith('!'):
                # Ako počinje s !, to je isključeni stream
                stream_name = selection[1:].split('.')[0]  # Makni ! i uzmi ime streama
                excluded.add(stream_name)
            else:
                # Inače je selektirani stream
                stream_name = selection.split('.')[0]
                selected.add(stream_name)
        
        self.logger.info(f"Selected streams: {selected}")
        self.logger.info(f"Excluded streams: {excluded}")
        
        # Filtriraj streamove
        filtered_streams = []
        for stream in available_streams:
            if stream.name in excluded:
                self.logger.info(f"Stream {stream.name} is explicitly excluded")
                continue
            if stream.name in selected:
                stream.selected = True
                filtered_streams.append(stream)
                self.logger.info(f"Stream {stream.name} is selected")
            else:
                self.logger.info(f"Stream {stream.name} is not selected")
        
        return filtered_streams


if __name__ == "__main__":
    TapKlaviyo.cli()
