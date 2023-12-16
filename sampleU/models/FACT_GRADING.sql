with
    stg_person as (
        select
            personid,
            {{ dbt_utils.generate_surrogate_key(["PERSON.PersonID"]) }} as personkey,
            concat(firstname, '  ', lastname) as studentname
        from {{ source("SAMPLEU", "PERSON") }}
    ),

    stg_studentgrade as (
        select
            courseid,
            enrollmentid,
            studentid,
            {{ dbt_utils.generate_surrogate_key(["STUDENTGRADE.EnrollmentID"]) }}
            as enrollmentkey,
            grade
        from {{ source("SAMPLEU", "STUDENTGRADE") }}
        group by enrollmentid, enrollmentkey, grade, courseid, studentid
    ),

    stg_course as (
        select
            courseid,
            {{ dbt_utils.generate_surrogate_key(["COURSE.CourseID"]) }} as coursekey,
            credits
        from {{ source("SAMPLEU", "COURSE") }}
    ),

    cred as (
        select sg.studentid, sg.grade, c.credits, sum(sg.grade * c.credits) as total
        from stg_studentgrade sg
        left join stg_course c on sg.courseid = c.courseid
        group by sg.studentid, sg.grade, c.credits
    ),
    stg_gpa as (
        select cc.studentid, round(sum(cc.total) / (sum(cc.credits)), 2) as gpa
        from cred cc
        group by cc.studentid
    )

select
    p.studentname,
    sg.enrollmentid,
    max(sg.enrollmentkey) as enrollmentkey,
    max(c.coursekey) as coursekey,
    count(distinct sg.enrollmentkey) as course_count,
    gc.gpa,
    sum(c.credits) as totalcredits,
    max(p.personkey) as personkey
from stg_studentgrade sg
join stg_person p on p.personid = sg.studentid
join stg_course c on c.courseid = sg.courseid
left join stg_gpa gc on gc.studentid = sg.studentid
group by sg.enrollmentid, p.studentname, gc.gpa
