"""
Sanity tests on the Gold layer output. Run after pipeline/gold_aggregate.py.

These mirror what the dbt schema tests enforce in the warehouse, but run
directly against the local CSV output so they can execute in CI without a
live Snowflake connection.
"""

import pandas as pd
from pathlib import Path

GOLD_DIR = Path(__file__).resolve().parents[1] / "data" / "gold"


def load_coverage():
    return pd.read_csv(GOLD_DIR / "employee_skill_coverage.csv")


def test_no_duplicate_employee_skill_pairs():
    df = load_coverage()
    dupes = df.duplicated(subset=["employee_id", "skill"]).sum()
    assert dupes == 0, f"Found {dupes} duplicate (employee_id, skill) rows"


def test_skill_completed_is_binary():
    df = load_coverage()
    assert set(df["skill_completed"].unique()).issubset({0, 1})


def test_quiz_score_in_valid_range():
    df = load_coverage()
    scored = df["best_quiz_score"].dropna()
    assert (scored >= 0).all() and (scored <= 100).all()


def test_no_null_employee_ids():
    df = load_coverage()
    assert df["employee_id"].isna().sum() == 0


if __name__ == "__main__":
    test_no_duplicate_employee_skill_pairs()
    test_skill_completed_is_binary()
    test_quiz_score_in_valid_range()
    test_no_null_employee_ids()
    print("All Gold layer tests passed.")
