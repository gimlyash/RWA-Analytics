from __future__ import annotations

import os
import sys
from pathlib import Path


def main() -> None:
    # Ensure repo root is on PYTHONPATH so "backend.*" imports work when running:
    #   python backend/manage.py ...
    repo_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo_root))

    os.environ.setdefault(
        "DJANGO_SETTINGS_MODULE", "backend.rwa_analytics_config.settings"
    )
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
