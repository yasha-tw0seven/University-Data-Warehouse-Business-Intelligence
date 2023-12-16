with
    f_grading as (select * from {{ ref('FACT_GRADING') }}),
    d_person as (select * from {{ ref('DIM_PERSON') }}),
    d_studentgrade as (select * from {{ ref('DIM_STUDENTGRADE') }}),
    d_course as (select * from {{ ref('dim_course') }})


select
    d_person.*,
    d_studentgrade.*,
    d_course.CourseID AS CourseID_d_course,
    d_course.Coursekey,
    f.course_count,
    f.TotalCredits,
    f.GPA
from f_grading as f
left join d_person on f.personkey = d_person.personkey
left join d_studentgrade on f.EnrollmentKey = d_studentgrade.Enrollmentkey
left join d_course on f.Coursekey = d_course.Coursekey