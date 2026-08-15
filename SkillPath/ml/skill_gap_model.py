"""
Skill-gap risk model.

Frames a next-best-action problem: for each (employee, skill) pair that a
role is expected to need but the employee has NOT completed, predict how
likely the employee is to actually finish that training if enrolled, based
on their completion behavior on other skills. This is the same "who should
we spend outreach effort on" shape as a churn or conversion model, applied
to L&D instead of a marketplace.

A random forest classifier is used for the same reason it's used in
ReadmitRisk: interpretable feature importances matter more here than
squeezing out marginal accuracy, since an L&D or HR stakeholder needs to
trust and explain *why* someone is flagged, not just the score itself.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.preprocessing import LabelEncoder
import joblib

BASE_DIR = Path(__file__).resolve().parents[1]
GOLD_DIR = BASE_DIR / "data" / "gold"
MODEL_DIR = BASE_DIR / "ml" / "artifacts"


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds one row per employee (not per employee-skill), since the model
    predicts a general "completion likelihood" propensity from an
    employee's overall training behavior, then that propensity is applied
    per-skill-gap at scoring time.
    """
    agg = df.groupby("employee_id").agg(
        department=("department", "first"),
        role_title=("role_title", "first"),
        total_attempts=("attempts", "sum"),
        skills_completed=("skill_completed", "sum"),
        skills_touched=("skill", "nunique"),
        avg_quiz_score=("best_quiz_score", "mean"),
        avg_days_to_complete=("fastest_days_to_complete", "mean"),
    ).reset_index()

    agg["completion_rate"] = agg["skills_completed"] / agg["skills_touched"].replace(0, np.nan)
    agg["completion_rate"] = agg["completion_rate"].fillna(0)
    agg["avg_quiz_score"] = agg["avg_quiz_score"].fillna(agg["avg_quiz_score"].median())
    agg["avg_days_to_complete"] = agg["avg_days_to_complete"].fillna(agg["avg_days_to_complete"].median())

    # Label: "high completer" = completion_rate above the population median.
    # This is what a real deployment would eventually replace with an actual
    # forward-looking label once enough historical enrollment cycles exist.
    threshold = agg["completion_rate"].median()
    agg["high_completer"] = (agg["completion_rate"] > threshold).astype(int)

    return agg


def train():
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(GOLD_DIR / "employee_skill_coverage.csv")
    features = build_features(df)

    le_dept = LabelEncoder()
    le_role = LabelEncoder()
    features["department_enc"] = le_dept.fit_transform(features["department"])
    features["role_enc"] = le_role.fit_transform(features["role_title"])

    feature_cols = [
        "department_enc", "role_enc", "total_attempts", "skills_touched",
        "avg_quiz_score", "avg_days_to_complete",
    ]
    X = features[feature_cols]
    y = features["high_completer"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = RandomForestClassifier(
        n_estimators=200, max_depth=6, min_samples_leaf=5, random_state=42
    )
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    probs = model.predict_proba(X_test)[:, 1]

    print("=== Skill-Gap Completion Propensity Model ===")
    print(classification_report(y_test, preds))
    print(f"ROC AUC: {roc_auc_score(y_test, probs):.3f}")

    importances = pd.Series(model.feature_importances_, index=feature_cols).sort_values(ascending=False)
    print("\nFeature importances:")
    print(importances.to_string())

    joblib.dump(model, MODEL_DIR / "skill_gap_model.joblib")
    joblib.dump(le_dept, MODEL_DIR / "department_encoder.joblib")
    joblib.dump(le_role, MODEL_DIR / "role_encoder.joblib")
    print(f"\nModel artifacts saved to {MODEL_DIR}")

    return model, features


if __name__ == "__main__":
    train()
