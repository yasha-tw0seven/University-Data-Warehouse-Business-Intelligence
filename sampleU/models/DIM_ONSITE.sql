with stg_onsite as (select * from {{ source("SAMPLEU", "ONSITECOURSE") }})
select
    {{ dbt_utils.generate_surrogate_key(["stg_onsite.COURSEID"]) }} as departmentkey,
    courseid,
    location,
    days,
    case
        when days = 'MWHF'
        then 'Monday,Wednesday, Thursday, Friday'
        when days = 'MTWH'
        then 'Monday, Tuesday, Wednesday, Thursday'
        when days = 'TWHF'
        then 'Tuesday, Wednesday, Thursday, Friday'
        when days = 'MTWH'
        then 'Monday, Tuesday, Wednesday, Thursday'
        when days = 'MWF'
        then 'Monday, Wednesday, Friday'
        when days = 'TH'
        then 'Tuesday, Thursday'
    end as classdays
from stg_onsite
