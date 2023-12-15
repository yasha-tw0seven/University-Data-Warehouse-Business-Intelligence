with
    stg_onlmode as (
        select
            {{ dbt_utils.generate_surrogate_key(["ONLINECOURSE.COURSEID"]) }}
            as modekey,
            *
        from {{ source("SAMPLEU", "ONLINECOURSE") }}
    ),
    stg_onsmode as (
        select
            {{ dbt_utils.generate_surrogate_key(["ONSITECOURSE.COURSEID"]) }}
            as modekey,
            *
        from {{ source("SAMPLEU", "ONSITECOURSE") }}
    )

select
    ac.courseid,
    ac.mode,
    case
        when ac.mode = 'online' then om.modekey when ac.mode = 'onsite' then os.modekey
    end as modekey
from
    (
        select courseid, 'online' as mode
        from stg_onlmode
        union
        select courseid, 'onsite' as mode
        from stg_onsmode
    ) as ac
left join stg_onlmode om on ac.courseid = om.courseid and ac.mode = 'online'
left join stg_onsmode os on ac.courseid = os.courseid and ac.mode = 'onsite'
