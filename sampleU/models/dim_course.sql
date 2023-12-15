with stg_course as (select * from {{ source("SAMPLEU", "COURSE") }})
select
    {{ dbt_utils.generate_surrogate_key(["stg_course.CourseID"]) }} as coursekey,
    courseid,
    departmentid,
    title,
    credits
from stg_course
