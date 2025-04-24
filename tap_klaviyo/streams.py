"""Stream type classes for tap-klaviyo."""

from __future__ import annotations

import typing as t
from pathlib import Path
from datetime import datetime, UTC, timezone

from singer_sdk import typing as th
from tap_klaviyo.client import DEFAULT_START_DATE, KlaviyoStream

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


class CampaignValuesReportsStream(KlaviyoStream):
    """Campaign Values Reports stream."""

    name = "campaign_values_reports"
    path = "/campaign-values-reports"
    primary_keys = ["id", "campaign_id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_values_reports.json"

    def prepare_request_payload(
        self, context: dict | None, next_page_token: t.Any | None,
    ) -> dict | None:
        """Prepare the request payload."""
        if not context:
            context = {}
        
        # Uzimamo start_date iz konfiguracije ili koristimo vlastitu default vrednost
        if "start_date" in self.config:
            start_date = self.config["start_date"]
            if isinstance(start_date, str):
                try:
                    dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                except ValueError:
                    dt = datetime.strptime(start_date, "%Y-%m-%d")
                start_date = dt.replace(tzinfo=timezone.utc).isoformat()
        else:
            default_date = datetime(2000, 1, 1, tzinfo=timezone.utc)
            start_date = default_date.isoformat()
        
        # Za end_date koristimo trenutni timestamp
        end_date = datetime.now(timezone.utc).isoformat()
        
        # Tačno ista struktura kao u vašem Postman primeru
        return {
            "data": {
                "type": "campaign-values-report",
                "attributes": {
                    "timeframe": {
                        "start": start_date,
                        "end": end_date
                    },
                    "statistics": [
                        "bounced",
                        "bounced_or_failed",
                        "clicks",
                        "click_rate",
                        "conversions",
                        "delivered",
                        "failed",
                        "opens",
                        "recipients",
                        "unsubscribes"
                    ],
                    "conversion_metric_id": self.config.get("conversion_metric_id", "RjtB2B")
                }
            }
        }

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        """Get records from stream source."""
        if not self.selected:
            return
        
        # Postavljamo potrebne HTTP headers
        headers = {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
        }
        
        # Koristimo revision iz konfiguracije ako postoji, inače default vrednost
        if "revision" in self.config:
            headers["Revision"] = self.config["revision"]
        else:
            headers["Revision"] = "2025-04-15"  # Default vrednost ako revision nije u konfiguraciji
        
        # Dodajemo autentikacioni header
        authenticator = self.authenticator
        if authenticator:
            for key, value in authenticator.auth_headers.items():
                headers[key] = value
        
        # Pripremamo payload
        payload = self.prepare_request_payload(context, None)
        
        # Šaljemo POST request
        http_method = "POST"
        self.logger.info(f"Making {http_method} request to {self.url_base}{self.path}")
        
        try:
            # Ispisujemo URL i headere za debug
            self.logger.info(f"Request URL: {self.url_base}{self.path}")
            self.logger.info(f"Request headers: {headers}")
            self.logger.info(f"Request payload: {payload}")
            
            response = self.requests_session.request(
                method=http_method,
                url=f"{self.url_base}{self.path}",
                headers=headers,
                json=payload
            )
            
            # Ispisujemo status kod i response za debug
            self.logger.info(f"Response status code: {response.status_code}")
            if response.status_code != 200:
                self.logger.error(f"Response content: {response.content}")
            
            response.raise_for_status()
            
            # Obrađujemo odgovor
            response_json = response.json()
            
            # Proveravamo da li imamo potrebnu strukturu u odgovoru
            if (
                "data" in response_json and
                "attributes" in response_json["data"] and
                "results" in response_json["data"]["attributes"]
            ):
                report_id = response_json["data"]["id"]
                
                # Za svaki rezultat u 'results' nizu kreiramo jedan zapis
                for result in response_json["data"]["attributes"]["results"]:
                    # Izvlačimo groupings i statistics
                    groupings = result.get("groupings", {})
                    statistics = result.get("statistics", {})
                    
                    # Kreiramo zapis koji kombinuje sve potrebne podatke
                    record = {
                        "id": report_id,
                        "campaign_id": groupings.get("campaign_id"),
                        "campaign_message_id": groupings.get("campaign_message_id"),
                        "send_channel": groupings.get("send_channel"),
                        # Dodajemo svu statistiku direktno na koren zapisa
                        **statistics
                    }
                    
                    yield record
            else:
                self.logger.warning(f"Unexpected response format: {response_json}")
                
        except Exception as e:
            self.logger.error(f"Error fetching campaign values report: {str(e)}")
            self.logger.error(f"Exception details: {type(e).__name__}: {str(e)}")
            raise
