with stg_courseinstructor as (select * from {{ source("SAMPLEU", "COURSEINSTRUCTOR") }})

select
    {{ dbt_utils.generate_surrogate_key(["stg_courseinstructor.courseid"]) }}
    as courseinstructorkey,
    courseid,
    personid
from stg_courseinstructor
