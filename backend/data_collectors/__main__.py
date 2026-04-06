"""Из корня репозитория: python -m backend.data_collectors"""

from __future__ import annotations

import argparse
import logging
import sys

from dotenv import load_dotenv

from backend.data_collectors.config import load_collector_settings
from backend.data_collectors.service import DataCollectorService


def main() -> int:
    load_dotenv()
    p = argparse.ArgumentParser(description="RWA Analytics: сбор DeFiLlama → JSON bundle / PostgreSQL")
    p.add_argument("--no-save", action="store_true", help="Не писать data/raw/*.json")
    p.add_argument(
        "--no-save-db",
        action="store_true",
        help="Не писать в PostgreSQL",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    svc = DataCollectorService(settings=load_collector_settings())
    save_db = not args.no_save_db
    if args.no_save:
        if svc.settings.database_url and save_db:
            bundle = svc.collect_and_save_database_only()
            print("Saved to PostgreSQL")
        else:
            bundle = svc.collect()
    else:
        bundle, path = svc.collect_and_save_json(save_database=save_db)
        print(f"Saved: {path}")
        if save_db and svc.settings.database_url:
            print("Saved to PostgreSQL")

    ok_n = sum(1 for s in bundle.sources if s.ok)
    print(f"Sources OK: {ok_n}/{len(bundle.sources)}")
    for s in bundle.sources:
        extra = f" — {s.error}" if s.error else ""
        print(f"  [{'OK' if s.ok else 'FAIL'}] {s.source}{extra}")
    return 0 if ok_n == len(bundle.sources) else 1


if __name__ == "__main__":
    sys.exit(main())
