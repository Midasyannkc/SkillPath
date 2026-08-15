-- Skill coverage is computed at the (employee, skill) grain rather than
-- (employee, course), because an employee can complete two different
-- courses that both map to the same target skill. Modeling at the skill
-- grain is what makes "who has a Python gap" answerable without a second
-- join or CASE statement in every downstream query or dashboard.

with completions as (
    select * from {{ ref('stg_completions') }}
),

courses as (
    select * from {{ ref('stg_courses') }}
),

employees as (
    select * from {{ ref('stg_employees') }}
),

enriched as (
    select
        c.employee_id,
        e.department,
        e.role_title,
        co.target_skill as skill,
        c.status,
        c.quiz_score,
        c.days_to_complete
    from completions c
    left join courses co on c.course_id = co.course_id
    left join employees e on c.employee_id = e.employee_id
)

select
    employee_id,
    department,
    role_title,
    skill,
    max(case when status = 'completed' then 1 else 0 end) as skill_completed,
    max(quiz_score)                                        as best_quiz_score,
    min(days_to_complete)                                   as fastest_days_to_complete,
    count(*)                                                as attempts
from enriched
group by 1, 2, 3, 4
