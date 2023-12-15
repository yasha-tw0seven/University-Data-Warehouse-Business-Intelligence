with stg_person as (select * from {{ source("SAMPLEU", "PERSON") }})

select
    {{ dbt_utils.generate_surrogate_key(["stg_person.PERSONID"]) }} as personkey,
    personid,
    concat(lastname, ', ', firstname) as personlastfirst,
    concat(firstname, ' ', lastname) as personfirstlast,
    hiredate as personhiredate,
    enrollmentdate as personenrollmentdate,
    discriminator as persondiscriminator
from stg_person
