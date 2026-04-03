"""Сбор данных: сейчас DeFiLlama; оркестратор — DataCollectorService."""

from backend.data_collectors.service import DataCollectorService, collect_all

__all__ = ["DataCollectorService", "collect_all"]
