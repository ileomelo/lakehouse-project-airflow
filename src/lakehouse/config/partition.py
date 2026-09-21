"""Utilitários para determinar a partição diária dos dados."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

LOCAL_TIMEZONE = ZoneInfo("America/Sao_Paulo")


def current_partition_date() -> str:
    """Retorna a data atual no fuso de negócio do lakehouse."""
    return datetime.now(LOCAL_TIMEZONE).date().isoformat()
