with
    stg_person as (
        select
            personid,
            {{ dbt_utils.generate_surrogate_key(["PERSON.PersonID"]) }} as personkey,
            concat(firstname, '  ', lastname) as adminname,
            discriminator
        from {{ source("SAMPLEU", "PERSON") }}
    ),

    stg_department as (
        select
            departmentid,
            {{ dbt_utils.generate_surrogate_key(["DEPARTMENT.DepartmentID"]) }}
            as departmentkey,
            name,
            sum(budget) as totalbudget,
            administrator
        from {{ source("SAMPLEU", "DEPARTMENT") }}
        group by departmentid, name, administrator
    )

select
    d.departmentid,
    max(d.departmentkey) as departmentkey,
    d.name,
    p.personid,
    max(p.personkey) as personkey,
    p.adminname,
    p.discriminator,
    max(d.totalbudget) as totalbudget
from stg_department d
join stg_person p on p.personid = d.administrator
group by p.personid, p.adminname, d.departmentid, d.name, p.discriminator
