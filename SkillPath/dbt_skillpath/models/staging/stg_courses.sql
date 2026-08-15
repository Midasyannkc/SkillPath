with source as (
    select * from {{ source('bronze', 'course_catalog') }}
)

select
    course_id,
    course_title,
    target_skill,
    cast(estimated_hours as integer) as estimated_hours
from source
