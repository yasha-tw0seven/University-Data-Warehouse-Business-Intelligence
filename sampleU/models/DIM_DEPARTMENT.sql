with stg_department as (select * from {{ source("SAMPLEU", "DEPARTMENT") }})

select
    {{ dbt_utils.generate_surrogate_key(["stg_department.departmentid"]) }}
    as departmentkey,
    departmentid,
    name as departmentname,
    budget as departmentbudget,
    startdate,
    administrator as administratorid
from stg_department
