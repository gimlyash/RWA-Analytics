"""
Основа сбора данных из открытых API (расширяемый каркас).

Сейчас подключён пример: DeFiLlama. Дальше — CoinGecko, Dune, Chainlink и т.д. по тому же шаблону.
"""

from backend.data_collectors.service import DataCollectorService, collect_all

__all__ = ["DataCollectorService", "collect_all"]
