"""
Gold layer: business-ready tables for reporting and the skill-gap model.

Produces:
  - gold_employee_skill_coverage : one row per (employee, skill) with
    completion status, score, and days-to-complete -- the canonical table
    any dashboard or the ML model reads from.
  - gold_department_training_summary : department-level rollup for
    manager-facing reporting.

Technical decision: skill coverage is computed as MAX(completed) per
(employee, skill) rather than per (employee, course), since an employee may
complete multiple courses mapped to the same target skill (e.g. two
Python courses). Modeling at the skill grain, not the course grain, is what
makes the "who has a Python gap" question answerable without a second join
at query time.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"


def get_spark():
    return (
        SparkSession.builder
        .appName("SkillPath-Gold")
        .master("local[*]")
        .getOrCreate()
    )


def run():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    spark = get_spark()

    employees = spark.read.parquet(str(SILVER_DIR / "employees"))
    courses = spark.read.parquet(str(SILVER_DIR / "courses"))
    completions = spark.read.parquet(str(SILVER_DIR / "completions"))

    enriched = (
        completions
        .join(courses, "course_id", "left")
        .join(employees, "employee_id", "left")
    )

    skill_coverage = (
        enriched.groupBy("employee_id", "department", "role_title", "target_skill")
        .agg(
            F.max(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("skill_completed"),
            F.max("quiz_score").alias("best_quiz_score"),
            F.min("days_to_complete").alias("fastest_days_to_complete"),
            F.count("*").alias("attempts"),
        )
        .withColumnRenamed("target_skill", "skill")
    )

    dept_summary = (
        enriched.groupBy("department")
        .agg(
            F.countDistinct("employee_id").alias("employees_engaged"),
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completions"),
            F.round(F.avg("quiz_score"), 1).alias("avg_quiz_score"),
        )
    )

    skill_coverage.write.mode("overwrite").parquet(str(GOLD_DIR / "employee_skill_coverage"))
    dept_summary.write.mode("overwrite").parquet(str(GOLD_DIR / "department_training_summary"))

    # Also write a flat CSV for the ML step and for easy inspection
    skill_coverage.toPandas().to_csv(GOLD_DIR / "employee_skill_coverage.csv", index=False)
    dept_summary.toPandas().to_csv(GOLD_DIR / "department_training_summary.csv", index=False)

    print(f"Gold employee_skill_coverage: {skill_coverage.count()} rows")
    print(f"Gold department_training_summary: {dept_summary.count()} rows")

    spark.stop()


if __name__ == "__main__":
    run()
