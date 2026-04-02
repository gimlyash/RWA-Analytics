"""
Запуск из корня репозитория::

    python -m backend.data_collectors
"""

from __future__ import annotations

import argparse
import logging
import sys

from backend.data_collectors.config import load_collector_settings
from backend.data_collectors.service import DataCollectorService


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="RWA Analytics: сбор данных (каркас; источники в service.runners).",
    )
    p.add_argument(
        "--no-save",
        action="store_true",
        help="Не записывать JSON в data/raw",
    )
    p.add_argument("-v", "--verbose", action="store_true", help="Подробный лог")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    settings = load_collector_settings()
    service = DataCollectorService(settings=settings)

    if args.no_save:
        bundle = service.collect()
    else:
        bundle, path = service.collect_and_save_json()
        print(f"Saved: {path}")

    ok_count = sum(1 for s in bundle.sources if s.ok)
    print(f"Sources OK: {ok_count}/{len(bundle.sources)}")
    for s in bundle.sources:
        status = "OK" if s.ok else "FAIL"
        err = f" — {s.error}" if s.error else ""
        print(f"  [{status}] {s.source}{err}")

    return 0 if ok_count == len(bundle.sources) else 1


if __name__ == "__main__":
    sys.exit(main())
