with
    f_departmentbudget as (select * from {{ ref("FACT_DEPARTMENTBUDGET") }}),
    d_person as (select * from {{ ref("DIM_PERSON") }}),
    d_department as (select * from {{ ref("DIM_DEPARTMENT") }})

select d_person.*, d_department.*, totalbudget
from f_departmentbudget as f
left join d_person on f.personkey = d_person.personkey
left join d_department on f.departmentkey = d_department.departmentkey
