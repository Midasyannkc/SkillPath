# SkillPath

A training-effectiveness and skill-gap data pipeline, built on a Medallion
architecture (bronze/silver/gold), governed with dbt, orchestrated with
Airflow, and feeding a skill-gap completion propensity model.

Simulates the kind of data platform an L&D or People Analytics team runs on
top of an LMS (e.g. Cornerstone, Docebo) and HRIS (e.g. Workday): course
completions, quiz scores, and employee/department context, joined into a
single governed table that answers "who has a skill gap, and how likely are
they to close it if we enroll them."

## Why this exists

Most portfolio data pipelines model e-commerce or finance data. This one
models workforce/L&D data instead, on purpose, because that domain has its
own real engineering problems: messy upstream HRIS department names, LMS
webhook retries that double-fire completion events, and a status field
that can disagree with the completion date it's supposed to reflect. The
pipeline is built to catch all three by construction, not by accident.

## Architecture

```
Bronze (raw CSV)          Silver (PySpark)              Gold (PySpark)
─────────────────         ──────────────────            ──────────────────
hris_employees.csv    →   cleaned, deduped employees  →  employee_skill_coverage
course_catalog.csv    →   cleaned course catalog       →  department_training_summary
lms_completions.csv   →   deduped, status-reconciled
                           completions
                                    │
                                    ▼
                          dbt (staging → marts, tested)
                                    │
                                    ▼
                          Skill-gap completion
                          propensity model (sklearn)
```

Orchestrated end-to-end by `airflow/dags/skillpath_dag.py`:
`bronze_ingest → silver_transform → gold_aggregate → dbt_build → ml_scoring`

## Key technical decisions

- **Skill grain, not course grain.** `fct_employee_skill_coverage` is built
  at (employee, skill), not (employee, course), because an employee can
  complete two different courses mapped to the same target skill. Modeling
  at the skill grain is what makes "who has a Python gap" answerable
  without a second join at query time.
- **De-duplication happens once, in Silver/staging.** LMS webhook retries
  occasionally double-fire a completion event. De-duplicating in Silver
  (PySpark) and again via a dbt `unique_combination_of_columns` test on
  the mart means a regression here fails loudly instead of quietly
  inflating a completion count on a dashboard.
- **Status is reconciled against `completed_date`, not trusted as-is.**
  Upstream LMS status fields and completion dates have been observed (in
  real systems, and simulated here) to disagree. The pipeline treats
  `completed_date IS NOT NULL` as the source of truth for "completed."
- **Random forest over a more complex model for skill-gap scoring.**
  Feature-importance interpretability matters more than marginal accuracy
  here, since an HR or L&D stakeholder needs to trust and explain *why*
  someone is flagged, not just receive a score.

## Repo layout

```
data_generation/     synthetic LMS + HRIS bronze data generator
pipeline/             PySpark silver_transform.py and gold_aggregate.py
dbt_skillpath/        dbt project: staging models, marts, schema tests
airflow/dags/         orchestration DAG
ml/                   skill-gap completion propensity model
data/                 bronze/silver/gold outputs (generated, not committed)
```

## Running it locally

```bash
pip install -r requirements.txt

# Bronze: generate synthetic LMS/HRIS data
python3 data_generation/generate_lms_hris_data.py

# Silver: clean, dedupe, join
python3 pipeline/silver_transform.py

# Gold: aggregate to skill-coverage and department summary tables
python3 pipeline/gold_aggregate.py

# dbt: build and test the governed mart layer (requires a warehouse target)
cd dbt_skillpath && dbt deps && dbt build

# ML: train the skill-gap completion propensity model
cd .. && python3 ml/skill_gap_model.py
```

## Stack

Python, PySpark, dbt, Apache Airflow, scikit-learn, Snowflake (dbt target).
