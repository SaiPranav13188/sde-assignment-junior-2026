#!/usr/bin/env python3
"""Entry point: python solution/pipeline.py <ingest|report> ... (works from any cwd)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from au_pipeline.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
