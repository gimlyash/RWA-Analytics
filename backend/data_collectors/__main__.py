"""Из корня репозитория: python -m backend.data_collectors"""

from __future__ import annotations

import argparse
import logging
import sys

from backend.data_collectors.config import load_collector_settings
from backend.data_collectors.service import DataCollectorService


def main() -> int:
    p = argparse.ArgumentParser(description="RWA Analytics: сбор DeFiLlama → JSON bundle")
    p.add_argument("--no-save", action="store_true", help="Не писать data/raw/*.json")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    svc = DataCollectorService(settings=load_collector_settings())
    if args.no_save:
        bundle = svc.collect()
    else:
        bundle, path = svc.collect_and_save_json()
        print(f"Saved: {path}")

    ok_n = sum(1 for s in bundle.sources if s.ok)
    print(f"Sources OK: {ok_n}/{len(bundle.sources)}")
    for s in bundle.sources:
        extra = f" — {s.error}" if s.error else ""
        print(f"  [{'OK' if s.ok else 'FAIL'}] {s.source}{extra}")
    return 0 if ok_n == len(bundle.sources) else 1


if __name__ == "__main__":
    sys.exit(main())
