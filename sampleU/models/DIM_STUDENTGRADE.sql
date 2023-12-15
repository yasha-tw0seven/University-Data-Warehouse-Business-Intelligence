with stg_studentgrade as (select * from {{ source("SAMPLEU", "STUDENTGRADE") }})

select
    {{ dbt_utils.generate_surrogate_key(["stg_studentgrade.ENROLLMENTID"]) }}
    as enrollmentkey,
    enrollmentid,
    studentid,
    courseid,
    grade
from stg_studentgrade
