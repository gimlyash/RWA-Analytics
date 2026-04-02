"""Ключевые слова и slug для отбора RWA в примере DeFiLlama — расширяйте под диплом."""

from __future__ import annotations

RWA_NAME_KEYWORDS: tuple[str, ...] = (
    "buidl",
    "ousg",
    "usdy",
    "ondo",
    "centrifuge",
    "maple",
    "rwa",
    "treasury",
)

RWA_PROTOCOL_SLUGS: frozenset[str] = frozenset(
    {"ondo-finance", "maple", "centrifuge", "superstate"},
)
