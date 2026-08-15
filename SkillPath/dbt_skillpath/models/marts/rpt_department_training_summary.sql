-- Manager-facing rollup. Deliberately built on top of fct_employee_skill_coverage
-- rather than querying stg_completions directly, so this report and the ML
-- model both inherit the same de-duplication and skill-grain logic instead
-- of drifting apart over time.

with coverage as (
    select * from {{ ref('fct_employee_skill_coverage') }}
)

select
    department,
    count(distinct employee_id)               as employees_engaged,
    sum(skill_completed)                      as completions,
    round(avg(best_quiz_score), 1)            as avg_quiz_score,
    round(avg(fastest_days_to_complete), 1)   as avg_days_to_complete
from coverage
group by 1
