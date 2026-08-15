-- Normalizes department casing/whitespace at the source, once, so every
-- downstream model and BI consumer reads the same canonical department
-- value instead of re-implementing this cleanup independently.

with source as (
    select * from {{ source('bronze', 'hris_employees') }}
)

select
    employee_id,
    initcap(trim(department))  as department,
    role_title,
    cast(hire_date as date)    as hire_date,
    nullif(manager_id, '')     as manager_id
from source
