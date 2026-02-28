"""
Elasticsearch ingestor.

Polls the ES REST API (_search) on a configured interval,
fetching documents newer than the last-seen timestamp.
Uses scroll for large result sets.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx

from common.schemas import LogEvent
from config.settings import get_settings
from ingestion.base import BaseIngestor, EventBus
from ingestion.normalizer import normalize_es_hit

logger = logging.getLogger(__name__)


class ElasticsearchIngestor(BaseIngestor):
    """
    Fetches payment log documents from Elasticsearch/Kibana indices.

    Query strategy:
      - On each poll, fetch docs where @timestamp > last_fetched_at
      - Uses `search_after` for stateless, cursor-based deep pagination
      - Falls back to time-range if no cursor exists
    """

    def __init__(self, bus: EventBus) -> None:
        super().__init__(bus)
        self._settings = get_settings().elasticsearch
        self._last_fetched_at: Optional[datetime] = None
        self._client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        headers = {"Content-Type": "application/json"}
        auth: Optional[tuple[str, str]] = None

        if self._settings.api_key:
            headers["Authorization"] = f"ApiKey {self._settings.api_key}"
        elif self._settings.username and self._settings.password:
            auth = (self._settings.username, self._settings.password)

        self._client = httpx.AsyncClient(
            base_url=self._settings.host,
            headers=headers,
            auth=auth,
            timeout=self._settings.timeout_seconds,
        )
        logger.info("ElasticsearchIngestor started → %s", self._settings.host)

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # BaseIngestor implementation
    # ------------------------------------------------------------------

    async def fetch(self) -> list[dict]:
        if self._client is None:
            await self.start()

        since = self._last_fetched_at or (
            datetime.now(timezone.utc)
            - timedelta(seconds=self._settings.poll_lookback_seconds)
        )
        until = datetime.now(timezone.utc)

        query = self._build_query(since, until)
        url = f"/{self._settings.index_pattern}/_search"

        try:
            resp = await self._client.post(url, json=query)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("ES query failed: %s %s", exc.response.status_code, exc.response.text[:200])
            return []
        except httpx.RequestError as exc:
            logger.error("ES connection error: %s", exc)
            return []

        data = resp.json()
        hits: list[dict] = data.get("hits", {}).get("hits", [])

        if hits:
            self._last_fetched_at = until
            logger.debug("ES fetched %d hits since %s", len(hits), since.isoformat())

        return hits

    async def normalize(self, raw_records: list[dict]) -> list[LogEvent]:
        events: list[LogEvent] = []
        for hit in raw_records:
            event = normalize_es_hit(hit)
            if event is not None:
                events.append(event)
        return events

    # ------------------------------------------------------------------
    # Query builder
    # ------------------------------------------------------------------

    def _build_query(self, since: datetime, until: datetime) -> dict[str, Any]:
        return {
            "size": self._settings.max_hits,
            "sort": [{"@timestamp": {"order": "asc"}}],
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": since.isoformat(),
                                    "lt": until.isoformat(),
                                }
                            }
                        }
                    ]
                }
            },
            "_source": True,
        }
