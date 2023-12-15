{{ config(materialized="table") }}
with stg_course as (select * from {{ source("SAMPLEU", "COURSE") }})
select courseid, departmentid, title, credits
from stg_course
