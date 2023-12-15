with stg_courseeval as (select * from {{ source("SAMPLEU", "COURSEEVALUATION") }})

select
    {{ dbt_utils.generate_surrogate_key(["stg_courseeval.EVAL_ID"]) }} as evalkey,
    course_id,
    sentiment,
    rating,
    evaluation
from stg_courseeval
