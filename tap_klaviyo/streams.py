"""Stream type classes for tap-klaviyo."""

from __future__ import annotations

import typing as t
from pathlib import Path

from singer_sdk import typing as th
from tap_klaviyo.client import KlaviyoStream

if t.TYPE_CHECKING:
    from urllib.parse import ParseResult

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class EventsStream(KlaviyoStream):
    """Define custom stream."""

    name = "events"
    path = "/events"
    primary_keys = ["id"]
    replication_key = "datetime"
    schema_filepath = SCHEMAS_DIR / "event.json"

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["datetime"] = row["attributes"]["datetime"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True

    def get_records(self, context: dict | None):
        """Get records from stream source."""
        if not self.selected:
            return
        yield from super().get_records(context)


class CampaignsStream(KlaviyoStream):
    """Define custom stream."""

    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "campaigns.json"

    @property
    def partitions(self) -> list[dict] | None:
        return [
            {
                "filter": "equals(messages.channel,'email')",
            },
            {
                "filter": "equals(messages.channel,'sms')",
            },
        ]

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, t.Any]:
        url_params = super().get_url_params(context, next_page_token)

        # Apply channel filters
        if context:
            parent_filter = url_params["filter"]
            url_params["filter"] = f"and({parent_filter},{context['filter']})"

        return url_params

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated_at"] = row["attributes"]["updated_at"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True

    def get_records(self, context: dict | None):
        """Get records from stream source."""
        if not self.selected:
            return
        yield from super().get_records(context)


class ProfilesStream(KlaviyoStream):
    """Define custom stream."""

    name = "profiles"
    path = "/profiles"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "profiles.json"
    max_page_size = 100

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True

    def get_records(self, context: dict | None):
        """Get records from stream source."""
        if not self.selected:
            return
        yield from super().get_records(context)


class MetricsStream(KlaviyoStream):
    """Metrics stream."""
    name = "metrics"
    path = "/metrics"
    primary_keys = ["id"]
    replication_key = None
    schema = th.ObjectType(
        th.Property("id", th.StringType),
        th.Property("attributes", th.ObjectType(
            th.Property("name", th.StringType),
            th.Property("integration", th.ObjectType(
                th.Property("name", th.StringType),
                th.Property("category", th.StringType),
                th.Property("object", th.StringType),
                th.Property("key", th.StringType),
            )),
            th.Property("created", th.DateTimeType),
            th.Property("updated", th.DateTimeType),
        )),
    ).to_dict()

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Post process row."""
        if row.get("attributes", {}).get("integration"):
            integration = row["attributes"]["integration"]
            if isinstance(integration.get("category"), dict):
                integration["category"] = integration["category"].get("category")
        return row

    def get_records(self, context: dict | None):
        """Get records from stream source."""
        if not self.selected:
            return
        yield from super().get_records(context)


class ListsStream(KlaviyoStream):
    """Define custom stream."""

    name = "lists"
    path = "/lists"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "lists.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        context = context or {}
        context["list_id"] = record["id"]

        return super().get_child_context(record, context)  # type: ignore[no-any-return]

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row

    def get_records(self, context: dict | None):
        """Get records from stream source."""
        if not self.selected:
            return
        yield from super().get_records(context)


class ListPersonStream(KlaviyoStream):
    """Define custom stream."""

    name = "listperson"
    path = "/lists/{list_id}/relationships/profiles/"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = ListsStream
    schema_filepath = SCHEMAS_DIR / "listperson.json"
    max_page_size = 1000

    def post_process(self, row: dict, context: dict) -> dict | None:
        row["list_id"] = context["list_id"]
        return row

    def get_records(self, context: dict | None):
        """Get records from stream source."""
        if not self.selected:
            return
        yield from super().get_records(context)


class FlowsStream(KlaviyoStream):
    """Define custom stream."""

    name = "flows"
    path = "/flows"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "flows.json"
    is_sorted = True

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row

    def get_records(self, context: dict | None):
        """Get records from stream source."""
        if not self.selected:
            return
        yield from super().get_records(context)


class TemplatesStream(KlaviyoStream):
    """Define custom stream."""

    name = "templates"
    path = "/templates"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "templates.json"

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True

    def get_records(self, context: dict | None):
        """Get records from stream source."""
        if not self.selected:
            return
        yield from super().get_records(context)
