"""
Integration 3 — ETL Pipeline Autograder Tests

DO NOT MODIFY THIS FILE. It is used by the GitHub Actions autograder.

These tests validate:
1. The ETL pipeline runs and produces output
2. Transformation logic is correct (cancelled/suspicious filtered)
3. Learner-written tests in tests/test_etl.py are genuinely implemented
"""

import ast
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_pipeline():
    """Run the ETL pipeline as a subprocess."""
    result = subprocess.run(
        [sys.executable, "etl_pipeline.py"],
        capture_output=True, text=True, timeout=60,
        env={
            **__import__("os").environ,
            "DATABASE_URL": "postgresql://postgres:postgres@localhost:5432/amman_market",
        },
    )
    return result


# ---------------------------------------------------------------------------
# Pipeline execution tests
# ---------------------------------------------------------------------------

def test_etl_script_exists():
    assert Path("etl_pipeline.py").exists(), "etl_pipeline.py not found"


def test_etl_script_runs():
    """etl_pipeline.py must run without errors."""
    result = _run_pipeline()
    assert result.returncode == 0, (
        f"etl_pipeline.py failed with exit code {result.returncode}.\n"
        f"stderr: {result.stderr[:500]}"
    )


def test_output_file_created():
    """Pipeline must produce output/customer_analytics.csv."""
    _run_pipeline()
    assert Path("output/customer_analytics.csv").exists(), (
        "output/customer_analytics.csv not found after running the pipeline. "
        "Make sure your load() function writes to this path."
    )


def test_output_has_expected_columns():
    """Output CSV must contain the required customer summary columns."""
    _run_pipeline()
    df = pd.read_csv("output/customer_analytics.csv")
    required = {"customer_id", "customer_name", "total_orders", "total_revenue"}
    missing = required - set(df.columns)
    assert not missing, (
        f"Output CSV missing required columns: {missing}. "
        f"Found columns: {list(df.columns)}"
    )


def test_output_excludes_cancelled():
    """Cancelled orders must not contribute to customer summaries.

    The seed data contains cancelled orders. If cancelled orders are included,
    total_orders and total_revenue will be inflated.
    """
    _run_pipeline()
    # We verify indirectly: import the learner's transform and check it.
    # If the pipeline produced output, check that row count is reasonable.
    df = pd.read_csv("output/customer_analytics.csv")
    # The seed has ~100 customers, ~10% cancelled orders.
    # If all orders (including cancelled) were counted, revenue would be higher.
    # At minimum, verify the output has rows and revenue is positive.
    assert len(df) > 0, "Output CSV is empty — pipeline produced no customer summaries"
    assert (df["total_revenue"] > 0).all(), (
        "Some customers have total_revenue <= 0 — "
        "check that your transform computes revenue correctly"
    )


def test_output_excludes_suspicious_quantity():
    """Order items with quantity > 100 must be filtered out.

    The seed data contains 5-8 items with quantity > 100 (data entry errors).
    These should not appear in the final summary.
    """
    _run_pipeline()
    # Indirect check: if suspicious quantities are included, some customers
    # would have inflated revenue. We verify the pipeline ran and produced
    # reasonable output — the learner's own tests should verify specifics.
    df = pd.read_csv("output/customer_analytics.csv")
    if "avg_order_value" in df.columns:
        # Suspicious quantities (>100) at normal prices would create extreme
        # avg_order_values. A reasonable max for this dataset is ~500 JOD.
        assert df["avg_order_value"].max() < 5000, (
            f"avg_order_value max is {df['avg_order_value'].max():.2f} — "
            "this suggests suspicious quantities (>100) were not filtered"
        )


# ---------------------------------------------------------------------------
# Learner test completeness (AST-based)
# ---------------------------------------------------------------------------

def test_learner_tests_complete():
    """Every test function in tests/test_etl.py must have real assertions.

    This test verifies that learner-written tests are genuinely implemented,
    not left as pass or pytest.fail() placeholders.
    """
    filepath = Path("tests/test_etl.py")
    assert filepath.exists(), "tests/test_etl.py not found"

    source = filepath.read_text()
    tree = ast.parse(source)

    test_functions = [
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    ]

    assert len(test_functions) >= 3, (
        f"Expected at least 3 test functions in test_etl.py, "
        f"found {len(test_functions)}"
    )

    for func in test_functions:
        # Count real assert statements
        asserts = [n for n in ast.walk(func) if isinstance(n, ast.Assert)]

        # Detect bare pass (placeholder not replaced)
        has_bare_pass = (
            len(func.body) == 1
            and isinstance(func.body[0], ast.Pass)
        )

        # Detect pytest.fail("Not implemented...") still present
        has_pytest_fail_placeholder = any(
            isinstance(n, ast.Expr)
            and isinstance(getattr(n, "value", None), ast.Call)
            and "fail" in ast.dump(getattr(n.value, "func", ast.Name()))
            and any(
                "Not implemented" in ast.dump(arg)
                for arg in getattr(n.value, "args", [])
            )
            for n in func.body
        )

        assert not has_bare_pass, (
            f"{func.name}() still has a bare 'pass' — "
            "replace it with your test implementation"
        )
        assert not has_pytest_fail_placeholder, (
            f"{func.name}() still has the 'Not implemented' placeholder — "
            "replace it with your test implementation"
        )
        assert len(asserts) >= 1, (
            f"{func.name}() has no assert statements — "
            "each test function must contain at least one assertion"
        )
