CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = PARQUET;

-- Students Stage
CREATE OR REPLACE STAGE ext_stage_silver_students
  URL = 'azure://udemydatalakestorage001.blob.core.windows.net/silver/Students/'
  CREDENTIALS = (AZURE_SAS_TOKEN = '***')
  FILE_FORMAT = parquet_format;

-- Courses Stage
CREATE OR REPLACE STAGE ext_stage_silver_courses
  URL = 'azure://udemydatalakestorage001.blob.core.windows.net/silver/courses/'
  CREDENTIALS = (AZURE_SAS_TOKEN = '***')
  FILE_FORMAT = parquet_format;

-- Enrollments Stage
CREATE OR REPLACE STAGE ext_stage_silver_enrollments
  URL = 'azure://udemydatalakestorage001.blob.core.windows.net/silver/Enrollment/'
  CREDENTIALS = (AZURE_SAS_TOKEN = '***')
  FILE_FORMAT = parquet_format;

-- Learning Stage
CREATE OR REPLACE STAGE ext_stage_silver_progress
  URL = 'azure://udemydatalakestorage001.blob.core.windows.net/silver/Learning/'
  CREDENTIALS = (AZURE_SAS_TOKEN = '***')
  FILE_FORMAT = parquet_format;

CREATE OR REPLACE EXTERNAL TABLE students_ext_raw
WITH LOCATION = @ext_stage_silver_students
FILE_FORMAT = parquet_format
AUTO_REFRESH = true;

SELECT * FROM students_ext_raw;

CREATE OR REPLACE VIEW vw_students AS
SELECT
  VALUE:"student_id"::INT AS student_id,
  VALUE:"name"::STRING AS name,
  VALUE:"email"::STRING AS email,
  VALUE:"signup_date"::DATE AS signup_date,
  VALUE:"location"::STRING AS location,
  VALUE:"is_current"::BOOLEAN AS is_current
FROM students_ext_raw
WHERE VALUE:"student_id" IS NOT NULL;

CREATE OR REPLACE VIEW vw_courses AS
SELECT
  VALUE:"course_id"::INT AS course_id,
  VALUE:"course_name"::STRING AS course_name,
  VALUE:"category"::STRING AS category,
  VALUE:"instructor"::STRING AS instructor,
  VALUE:"launch_date"::DATE AS launch_date,
  VALUE:"course_duration"::NUMBER(5,2) AS course_duration,
  VALUE:"is_current"::BOOLEAN AS is_current
FROM courses_ext_raw
WHERE VALUE:"course_id" IS NOT NULL;

CREATE OR REPLACE VIEW vw_enrollments AS
SELECT
  VALUE:"enrollment_id"::INT AS enrollment_id,
  VALUE:"student_id"::INT AS student_id,
  VALUE:"course_id"::INT AS course_id,
  VALUE:"enrollment_date"::DATE AS enrollment_date
FROM enrollments_ext_raw
WHERE VALUE:"enrollment_id" IS NOT NULL;


CREATE OR REPLACE VIEW vw_learning AS
SELECT
  VALUE:"log_id"::INT AS log_id,
  VALUE:"student_id"::INT AS student_id,
  VALUE:"course_id"::INT AS course_id,
  VALUE:"activity_type"::STRING AS activity_type,
  VALUE:"timestamp"::TIMESTAMP_NTZ AS activity_timestamp,
  VALUE:"duration_min"::NUMBER(10,2) AS duration_min
FROM learning_ext_raw
WHERE VALUE:"log_id" IS NOT NULL;

select * from vw_learning where student_id=1501 and course_id=18;
select * from vw_learning where student_id=1502 and course_id=10;

select * from vw_courses ;

CREATE OR REPLACE VIEW dim_student AS
SELECT
  student_id,
  name,
  email,
  signup_date,
  location,
  is_current AS student_is_current
FROM vw_students
WHERE is_current = TRUE;

CREATE OR REPLACE VIEW dim_course AS
SELECT
  course_id,
  course_name,
  category,
  instructor,
  launch_date,
  course_duration,
  is_current AS course_is_current
FROM vw_courses
WHERE is_current = TRUE;

CREATE OR REPLACE VIEW fact_engagement_summary AS
SELECT
  l.student_id,
  l.course_id,
  s.name,
  c.course_name,
  SUM(l.duration_min) AS total_time_spent,
  COUNT(l.log_id) AS activity_count,
  SUM(l.duration_min) * COUNT(l.log_id) AS engagement_score,
  MAX(l.activity_timestamp) AS last_activity,
  DATEDIFF('day', MAX(l.activity_timestamp), CURRENT_DATE) AS days_inactive,
  CASE 
    WHEN DATEDIFF('day', MAX(l.activity_timestamp), CURRENT_DATE) > 365 THEN 'At-Risk'
    ELSE 'Active' 
  END AS status,
  CASE 
    WHEN SUM(l.duration_min) >= c.course_duration THEN 'Completed'
    ELSE 'In-Progress'
  END AS completion_status
FROM vw_learning l
JOIN vw_students s ON l.student_id = s.student_id
JOIN vw_courses c ON l.course_id = c.course_id
JOIN vw_enrollments e 
  ON l.student_id = e.student_id 
  AND l.course_id = e.course_id
WHERE l.activity_timestamp >= c.launch_date
GROUP BY 
  l.student_id, l.course_id, s.name, c.course_name, c.course_duration;


CREATE OR REPLACE VIEW fact_enrollments AS
SELECT
  enrollment_id,
  student_id,
  course_id,
  enrollment_date
FROM vw_enrollments;

select * from fact_enrollments;

CREATE OR REPLACE EXTERNAL TABLE courses_ext_raw
WITH LOCATION = @ext_stage_silver_courses
FILE_FORMAT = parquet_format
AUTO_REFRESH = true;

CREATE OR REPLACE EXTERNAL TABLE enrollments_ext_raw
WITH LOCATION = @ext_stage_silver_enrollments
FILE_FORMAT = parquet_format
AUTO_REFRESH = TRUE;

CREATE OR REPLACE EXTERNAL TABLE learning_ext_raw
WITH LOCATION = @ext_stage_silver_progress
FILE_FORMAT = parquet_format
AUTO_REFRESH = True;

select * from enrollments_ext_raw;