# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_student=spark.read.format('delta').load('abfss://silver@udemydatalakestorage001.dfs.core.windows.net/Students')
df_student.display()

# COMMAND ----------

df_learn=spark.read.format('delta').load('abfss://silver@udemydatalakestorage001.dfs.core.windows.net/Learning')
df_learn.display()


# COMMAND ----------

df_courses=spark.read.format('delta').load('abfss://silver@udemydatalakestorage001.dfs.core.windows.net/courses')
df_courses.display()

# COMMAND ----------

df_enroll=spark.read.format('delta').load('abfss://silver@udemydatalakestorage001.dfs.core.windows.net/Enrollment')
df_enroll.display()

# COMMAND ----------

df_enroll.filter(col("student_id") == 1501) \
         .select("course_id", "enrollment_date") \
         .distinct() \
         .show(truncate=False)

# COMMAND ----------

df_enroll = df_enroll.withColumnRenamed("is_current", "enroll_is_current")
df_student = df_student.withColumnRenamed("is_current", "student_is_current")
df_courses = df_courses.withColumnRenamed("is_current", "course_is_current")

# COMMAND ----------

df_enroll= df_enroll.filter(col("is_current") == True)
df_student= df_student.filter(col("is_current") == True)
df_courses= df_courses.filter(col("is_current") == True)

# COMMAND ----------

df_learn.filter(col("student_id") == 21) \
        .select("course_id", "timestamp", "activity_type") \
        .distinct() \
        .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining 4 Tables

# COMMAND ----------

df_joined = df_learn \
    .join(df_enroll, ["student_id", "course_id"], "inner") \
    .join(df_student, "student_id", "left") \
    .join(df_courses, "course_id", "left")


# COMMAND ----------

df_joined=df_joined.drop('input_file','file_name')

# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_joined.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Spent on a course

# COMMAND ----------

from pyspark.sql.functions import sum as _sum
df_total_time = df_joined.groupBy(
    "student_id", "name"
).agg(
    _sum("duration_min").alias("total_time_spent")
).orderBy(desc("total_time_spent"))
df_total_time.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Average Session Duration per Student per Course

# COMMAND ----------

df_avg_session = df_joined.groupBy(
    "student_id", "name", "course_id", "course_name"
).agg(
    ( _sum("duration_min") / count("student_id") ).alias("avg_session_duration")
)
df_avg_session.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import sum as _sum, count
df_engagement = df_joined.groupBy(
    "student_id", "name", "course_id", "course_name"
).agg(
    _sum("duration_min").alias("total_time_spent"),
    count("student_id").alias("activity_count")
).withColumn(
    "engagement_score", col("total_time_spent") * col("activity_count")
)

df_engagement.select(
    "student_id", "name", "course_id", "course_name",
    "total_time_spent", "activity_count", "engagement_score"
).orderBy(desc('activity_count')).display()

# COMMAND ----------

from pyspark.sql.functions import max as _max, current_date, datediff, expr
df_status = df_joined.groupBy(
    "student_id", "name", "course_id", "course_name"
).agg(
    _max("timestamp").alias("last_activity")
)

df_status = df_status.withColumn(
    "days_inactive", datediff(current_date(), col("last_activity"))
).withColumn(
    "status", expr("CASE WHEN days_inactive > 365 THEN 'At-Risk' ELSE 'Active' END")
)

df_status.select(
    "student_id", "name", "course_id", "course_name", "last_activity",
    "days_inactive", "status"
).display()


# COMMAND ----------

fact_engagement_summary = df_joined.groupBy(
    "student_id", "course_id", "name", "course_name"
).agg(
    _sum("duration_min").alias("total_time_spent"),
    count("log_id").alias("activity_count"),
    (_sum("duration_min") * count("log_id")).alias("engagement_score"),
    _max("timestamp").alias("last_activity")
).withColumn(
    "days_inactive", datediff(current_date(), col("last_activity"))
).withColumn(
    "status", expr("CASE WHEN days_inactive > 365 THEN 'At-Risk' ELSE 'Active' END")
)

# COMMAND ----------

fact_engagement_summary = fact_engagement_summary.select(
    "student_id", "course_id",
    "total_time_spent", "activity_count", "engagement_score",
    "last_activity", "days_inactive", "status"
)

# COMMAND ----------

fact_enrollment = df_enroll.select(
    "enrollment_id", "student_id", "course_id", "enrollment_date","enroll_is_current"
).dropDuplicates(["enrollment_id"])


# COMMAND ----------

# Dimension: Students
dim_student = df_student.select(
    "student_id", "name", "email", "signup_date", "location", "student_is_current"
).dropDuplicates(["student_id"])

dim_course = df_courses.select(
    "course_id", "course_name", "category", "instructor", "launch_date", "course_duration", "course_is_current"
).dropDuplicates(["course_id"])


# COMMAND ----------

dbutils.secrets.list("snowflake-creds")

# COMMAND ----------

sfOptions = {
    "sfURL": "BGNOIXF-QOA74514.snowflakecomputing.com",
    "sfDatabase": "Udemy",
    "sfSchema": "gold",
    "sfWarehouse": "compute_wh",
    "sfRole": "accountadmin", 
    "sfUser": dbutils.secrets.get('snowflake-creds2','user'),
    "sfPassword": dbutils.secrets.get('snowflake-creds2','password')
}

# COMMAND ----------

dataframes = {
    "FACT_ENGAGEMENT_SUMMARY": fact_engagement_summary,
    "DIM_STUDENT": dim_student,
    "DIM_COURSE": dim_course,
    "FACT_ENROLLMENT": fact_enrollment
}

# COMMAND ----------

for table_name, df in dataframes.items():
    df.write.format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()

# COMMAND ----------

# fact_engagement_summary.write.format("snowflake") \
#     .options(**sfOptions) \
#     .option("dbtable", "FACT_ENGAGEMENT_SUMMARY") \
#     .mode("overwrite") \
#     .save()


# COMMAND ----------

# dim_student.write.format("snowflake") \
#     .options(**sfOptions) \
#     .option("dbtable", "DIM_STUDENT") \
#     .mode("overwrite") \
#     .save()

# COMMAND ----------

# dim_course.write.format("snowflake") \
#     .options(**sfOptions) \
#     .option("dbtable", "DIM_COURSE") \
#     .mode("overwrite") \
#     .save()

# COMMAND ----------

# fact_enrollment.write.format("snowflake") \
#     .options(**sfOptions) \
#     .option("dbtable", "FACT_ENROLLMENT") \
#     .mode("overwrite") \
#     .save()