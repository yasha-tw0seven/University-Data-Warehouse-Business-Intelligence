with
    stg_person as (
        select
            personid,
            {{ dbt_utils.generate_surrogate_key(["PERSON.personid"]) }} as personkey,
            concat(firstname, ' ', lastname) as instructorname
        from {{ source("SAMPLEU", "PERSON") }}
    ),

    stg_studentgrade as (
        select
            courseid,
            studentid,
            count(distinct studentid) as student_count,
            {{ dbt_utils.generate_surrogate_key(["STUDENTGRADE.EnrollmentID"]) }}
            as enrollmentkey,
            grade
        from {{ source("SAMPLEU", "STUDENTGRADE") }}
        group by enrollmentkey, grade, courseid, studentid
    ),

    stg_course as (
        select
            courseid,
            {{ dbt_utils.generate_surrogate_key(["COURSE.courseid"]) }} as coursekey,
            credits,
            title as coursename,
            departmentid
        from {{ source("SAMPLEU", "COURSE") }}
    ),

    stg_department as (
        select
            departmentid,
            {{ dbt_utils.generate_surrogate_key(["DEPARTMENT.DepartmentID"]) }}
            as departmentkey,
            name as departmentname
        from {{ source("SAMPLEU", "DEPARTMENT") }}
    ),

    stg_mode as (
        select
            courseid,
            {{ dbt_utils.generate_surrogate_key(["ONLINECOURSE.COURSEID"]) }}
            as modekey,
            'online' as mode
        from {{ source("SAMPLEU", "ONLINECOURSE") }}

        union all

        select
            courseid,
            {{ dbt_utils.generate_surrogate_key(["ONSITECOURSE.COURSEID"]) }}
            as modekey,
            'onsite' as mode
        from {{ source("SAMPLEU", "ONSITECOURSE") }}
    ),

    stg_instructor as (
        select
            {{ dbt_utils.generate_surrogate_key(["COURSEINSTRUCTOR.CourseID"]) }}
            as courseinstructorkey,
            personid,
            courseid
        from {{ source("SAMPLEU", "COURSEINSTRUCTOR") }}
    ),

    stg_eval as (
        select
            {{ dbt_utils.generate_surrogate_key(["COURSEEVALUATION.EVAL_ID"]) }}
            as evalkey,
            rating,
            sentiment,
            course_id as courseid
        from {{ source("SAMPLEU", "COURSEEVALUATION") }}
    ),

    stg_onsite as (
        select
            {{ dbt_utils.generate_surrogate_key(["ONSITECOURSE.COURSEID"]) }}
            as onsitekey,
            courseid,
            days
        from {{ source("SAMPLEU", "ONSITECOURSE") }}
    )

select
    c.courseid,
    c.coursename,
    d.departmentname,
    p.instructorname,
    max(m.mode) as mode,
    c.credits,
    case when max(o.days) like '%M%' then 'Yes' else 'No' end as monday,
    case when max(o.days) like '%T%' then 'Yes' else 'No' end as tuesday,
    case when max(o.days) like '%W%' then 'Yes' else 'No' end as wednesday,
    case when max(o.days) like '%H%' then 'Yes' else 'No' end as thursday,
    case when max(o.days) like '%F%' then 'Yes' else 'No' end as friday,
    count(distinct sg.studentid) as student_count,
    sum(sg.grade * c.credits * sg.student_count)
    / sum(c.credits * sg.student_count) as gpa,
    avg(e.rating) as ratingavg,
    case
        when count(e.sentiment) = 0
        then null  -- All sentiments are null
        when
            avg(
                case
                    when e.sentiment = 'Positive'
                    then 1.0
                    when e.sentiment = 'Negative'
                    then 0.0
                    else null
                end
            )
            >= 0.5
        then 'Positive'
        else 'Negative'
    end as sentiment,
    max(sg.enrollmentkey) as enrollmentkey,
    max(c.coursekey) as coursekey,
    max(d.departmentkey) as departmentkey,
    max(e.evalkey) as evalkey,
    max(m.modekey) as modekey,
    max(i.courseinstructorkey) as courseinstructorkey,
    max(p.personkey) as personkey,
    max(o.onsitekey) as onsitekey
from stg_course c
left join stg_studentgrade sg on c.courseid = sg.courseid
left join stg_department d on c.departmentid = d.departmentid
left join stg_mode m on c.courseid = m.courseid
left join stg_instructor i on c.courseid = i.courseid
left join stg_eval e on c.courseid = e.courseid
left join stg_person p on i.personid = p.personid
left join stg_onsite o on o.courseid = c.courseid
group by c.courseid, c.coursename, d.departmentname, p.instructorname, c.credits
