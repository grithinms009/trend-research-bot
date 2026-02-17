from __future__ import annotations

import abc
import logging
from typing import Any, Dict, List

from app.content_intelligence.models.topic import RawTopic

logger = logging.getLogger(__name__)


class ConnectorError(Exception):
    pass


class BaseConnector(abc.ABC):
    """Abstract connector interface for pluggable data sources."""

    name: str = "base"

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config or {}
        self.enabled: bool = self.config.get("enabled", True)

    async def fetch(self) -> List[RawTopic]:
        if not self.enabled:
            logger.info("Connector %s disabled via config", self.name)
            return []
        try:
            topics = await self._fetch_impl()
            return topics
        except Exception as err:  # pylint: disable=broad-except
            logger.exception("Connector %s failed: %s", self.name, err)
            raise ConnectorError(str(err))

    @abc.abstractmethod
    async def _fetch_impl(self) -> List[RawTopic]:
        raise NotImplementedError
