with
    f_courseanalysis as (select * from {{ ref("FACT_COURSEANALYSIS") }}),
    d_person as (select * from {{ ref("DIM_PERSON") }}),
    d_studentgrade as (select * from {{ ref("DIM_STUDENTGRADE") }}),
    d_course as (select * from {{ ref("dim_course") }}),
    d_mode as (select * from {{ ref("DIM_MODE") }}),
    d_department as (select * from {{ ref("DIM_DEPARTMENT") }}),
    d_eval as (select * from {{ ref("DIM_COURSEEVALUATION") }}),
    d_instructor as (select * from {{ ref("DIM_COURSEINSTRUCTOR") }})

select
    d_course.courseid,
    d_course.title as coursename,
    d_person.*,
    d_department.departmentname,
    d_mode.mode,
    d_course.credits,
    f.student_count,
    f.gpa,
    f.ratingavg,
    f.sentiment
from f_courseanalysis as f
left join d_person on f.personkey = d_person.personkey
left join d_studentgrade on f.enrollmentkey = d_studentgrade.enrollmentkey
left join d_course on f.coursekey = d_course.coursekey
left join d_department on f.departmentkey = d_department.departmentkey
left join d_mode on f.modekey = d_mode.modekey
left join d_instructor on f.courseinstructorkey = d_instructor.courseinstructorkey
left join d_eval on f.evalkey = d_eval.evalkey
