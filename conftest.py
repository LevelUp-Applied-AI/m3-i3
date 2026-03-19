# conftest.py — anchors pytest rootdir to repo root so that
# tests/ can import modules from the repo root (e.g., etl_pipeline.py).
import sys, os; sys.path.insert(0, os.path.abspath("."))
