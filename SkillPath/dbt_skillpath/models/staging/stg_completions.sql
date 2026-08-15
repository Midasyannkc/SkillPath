-- De-duplicates on the full event grain (LMS webhook retries fire the same
-- event twice on occasion) and reconciles status against completed_date,
-- since the two fields have been observed to disagree upstream.

with source as (
    select * from {{ source('bronze', 'lms_completions') }}
),

deduped as (
    select distinct * from source
),

cleaned as (
    select
        event_id,
        employee_id,
        course_id,
        cast(enrolled_date as date)  as enrolled_date,
        cast(nullif(completed_date, '') as date) as completed_date,
        try_cast(nullif(quiz_score, '') as float) as quiz_score,
        case
            when nullif(completed_date, '') is not null then 'completed'
            else status
        end as status
    from deduped
)

select
    *,
    case
        when completed_date is not null
        then datediff('day', enrolled_date, completed_date)
    end as days_to_complete
from cleaned
