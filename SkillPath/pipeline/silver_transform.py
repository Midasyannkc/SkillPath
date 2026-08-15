"""
Silver layer: cleans and joins raw bronze data.

Technical decisions worth noting:
  - Department names arrive inconsistently cased/spaced from HRIS
    ("Engineering", "engineering", "Sales "). Normalized once here so every
    downstream model and dbt test can rely on a single canonical value,
    instead of every consumer re-implementing the same .strip().title() fix.
  - Duplicate LMS completion events (webhook retries) are de-duplicated on
    the full event grain in Silver, not left for Gold or dbt to catch,
    because a duplicate here silently double-counts a training hour metric.
  - Completion status is normalized against completed_date/quiz_score rather
    than trusted blindly, since upstream "status" and "completed_date" can
    disagree (an LMS integration bug seen often enough in this domain to
    guard against by construction).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"


def get_spark():
    return (
        SparkSession.builder
        .appName("SkillPath-Silver")
        .master("local[*]")
        .getOrCreate()
    )


def clean_employees(spark):
    df = spark.read.csv(str(BRONZE_DIR / "hris_employees.csv"), header=True, inferSchema=True)
    df = df.withColumn("department_clean", F.initcap(F.trim(F.col("department"))))
    df = df.withColumn("hire_date", F.to_date("hire_date"))
    df = df.dropDuplicates(["employee_id"])
    return df.select(
        "employee_id", "department_clean", "role_title", "hire_date", "manager_id"
    ).withColumnRenamed("department_clean", "department")


def clean_courses(spark):
    df = spark.read.csv(str(BRONZE_DIR / "course_catalog.csv"), header=True, inferSchema=True)
    return df.dropDuplicates(["course_id"])


def clean_completions(spark):
    df = spark.read.csv(str(BRONZE_DIR / "lms_completions.csv"), header=True, inferSchema=True)

    # De-duplicate exact webhook-retry duplicates on the full event grain
    df = df.dropDuplicates(["employee_id", "course_id", "enrolled_date", "event_id"])

    df = df.withColumn("enrolled_date", F.to_date("enrolled_date"))
    df = df.withColumn("completed_date", F.to_date("completed_date"))
    df = df.withColumn("quiz_score", F.expr("try_cast(quiz_score AS DOUBLE)"))

    # Reconcile status against completed_date rather than trusting the raw
    # status field alone -- catches the case where a completion event fired
    # without the status field updating.
    df = df.withColumn(
        "status_clean",
        F.when(F.col("completed_date").isNotNull(), F.lit("completed"))
         .otherwise(F.col("status")),
    )

    df = df.withColumn(
        "days_to_complete",
        F.when(
            F.col("completed_date").isNotNull(),
            F.datediff("completed_date", "enrolled_date"),
        ),
    )

    return df.select(
        "event_id", "employee_id", "course_id", "enrolled_date",
        "completed_date", "quiz_score", "status_clean", "days_to_complete",
    ).withColumnRenamed("status_clean", "status")


def run():
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    spark = get_spark()

    employees = clean_employees(spark)
    courses = clean_courses(spark)
    completions = clean_completions(spark)

    employees.write.mode("overwrite").parquet(str(SILVER_DIR / "employees"))
    courses.write.mode("overwrite").parquet(str(SILVER_DIR / "courses"))
    completions.write.mode("overwrite").parquet(str(SILVER_DIR / "completions"))

    print(f"Silver employees:   {employees.count()} rows")
    print(f"Silver courses:     {courses.count()} rows")
    print(f"Silver completions: {completions.count()} rows (post de-dup)")

    spark.stop()


if __name__ == "__main__":
    run()
