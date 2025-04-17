USE DEC02ASSIGNMENT;


-- QUERY 1
-- Getting all 'A' grades from students of Economics and Big Data

with raw_data as (
    select
        json_extract_string(students_data, '$.student_id') as student_id,
        json_extract_string(students_data, '$.name') as student_name,
        json_extract_string(students_data, '$.study_program') as study_program,
        json_extract_string(students_data, '$.contact.email') as email,
        json_extract(students_data, '$.grades') as grades_json
    from read_csv_auto('d:/university/iii year/term 9/data engineering concepts/assignments/02/students_data.csv')
),

grades as (
    select
        student_id,
        student_name,
        study_program,
        email,
        unnest(json_extract_string(grades_json, '$[*].course')) as course,
        unnest(cast(json_extract(grades_json, '$[*].grade') as integer[])) as grade,
        unnest(cast(json_extract(grades_json, '$[*].credits') as integer[])) as credits
    from raw_data
)

select *
from grades
where lower(study_program) = 'economics and big data' and grade >= 91;




-- QUERY 2
-- Counting average number of ongoing courses per student by their study program

with student_data as (
    select
        json_extract_string(students_data, '$.student_id') as student_id,
        json_extract_string(students_data, '$.name') as student_name,
        cast(json_extract_string(students_data, '$.age') as integer) as age,
        json_extract_string(students_data, '$.study_program') as study_program,
        json_extract(students_data, '$.ongoing_courses') as ongoing_courses
    from read_csv_auto('d:/university/iii year/term 9/data engineering concepts/assignments/02/cleaned_students_data.csv')
),

course_expansion as (
    select
        student_id,
        student_name,
        age,
        study_program,
        unnest(json_extract_string(ongoing_courses, '$[*].course_name')) as course_name
    from student_data
),

course_counts as (
    select
        student_id,
        student_name,
        age,
        study_program,
        count(course_name) as total_courses
    from course_expansion
    group by student_id, student_name, age, study_program
)

select study_program, avg(total_courses) as avg_courses_per_student
from course_counts
group by study_program;