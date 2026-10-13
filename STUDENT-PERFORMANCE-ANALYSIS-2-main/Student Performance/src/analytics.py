# src/analytics.py
"""
Analytics Module containing Student Performance queries using the PySpark DataFrame API.

Why this file exists:
This is the analytical engine of our Big Data pipeline. It uses PySpark DataFrame 
transformations (select, filter, groupBy, orderBy, join) and aggregation functions (avg, count, min, max) 
to compute academic and administrative reports. Using Spark's distributed architecture, these 
operations can execute in parallel across large datasets.

Viva Tips:
- What is the difference between GroupBy and PartitionBy?
  - `groupBy` collapses rows to compute a single aggregated summary per group.
  - `partitionBy` (used in Window operations) defines group boundaries over which computations 
    (like ranks or rolling averages) are calculated, while keeping all original rows intact.
- What is a Window Function in Spark? It is a function that performs calculations across 
  a set of table rows that are related to the current row, defined using a `WindowSpec`.
- What are aggregations in Spark? Functions like `avg()`, `count()`, `sum()`, `max()`, and `min()` 
  that summarize data.
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import col, avg, count, sum, when, rank, desc, countDistinct
from pyspark.sql.window import Window

def get_top_10_students(df: DataFrame) -> DataFrame:
    """
    1. Top 10 Students based on Marks.
    Returns the top 10 scoring student records.
    """
    # Sorts the dataframe by the Marks column in descending order and limits the output to 10 rows.
    # orderBy() is a Spark transformation; limit() is a Spark transformation.
    return df.select("Student_ID", "Student_Name", "Department", "Subject", "Marks") \
             .orderBy(col("Marks").desc()) \
             .limit(10)

def get_highest_scorer(df: DataFrame) -> DataFrame:
    """
    2. Highest Scorer in the dataset.
    Returns the student record(s) matching the absolute maximum mark.
    """
    # Find the maximum marks value in the entire dataset.
    # first() is a Spark action that retrieves the first row.
    max_marks = df.select(F.max(col("Marks"))).first()[0]
    
    # Filter the dataframe to get students who scored this maximum mark.
    return df.filter(col("Marks") == max_marks) \
             .select("Student_ID", "Student_Name", "Department", "Subject", "Marks")

def get_lowest_scorer(df: DataFrame) -> DataFrame:
    """
    3. Lowest Scorer in the dataset.
    Returns the student record(s) matching the absolute minimum mark.
    """
    # Find the minimum marks value in the dataset.
    min_marks = df.select(F.min(col("Marks"))).first()[0]
    
    # Filter the dataframe to get students who scored this minimum mark.
    return df.filter(col("Marks") == min_marks) \
             .select("Student_ID", "Student_Name", "Department", "Subject", "Marks")

def get_subject_wise_average(df: DataFrame) -> DataFrame:
    """
    4. Subject-wise Average Marks.
    Groups records by Subject and calculates average marks.
    """
    # groupBy collapses rows by Subject.
    # agg() performs aggregate operations.
    # F.round() restricts floating-point values to 2 decimal places.
    return df.groupBy("Subject") \
             .agg(F.round(avg("Marks"), 2).alias("Average_Marks")) \
             .orderBy(col("Average_Marks").desc())

def get_department_wise_average(df: DataFrame) -> DataFrame:
    """
    5. Department-wise Average Marks.
    Groups records by Department and calculates average marks.
    """
    return df.groupBy("Department") \
             .agg(F.round(avg("Marks"), 2).alias("Average_Marks")) \
             .orderBy(col("Average_Marks").desc())

def get_semester_performance(df: DataFrame) -> DataFrame:
    """
    6. Semester-wise Performance.
    Calculates average Marks and average Attendance for each Semester.
    """
    return df.groupBy("Semester") \
             .agg(
                 F.round(avg("Marks"), 2).alias("Average_Marks"),
                 F.round(avg("Attendance"), 2).alias("Average_Attendance")
             ) \
             .orderBy("Semester")

def get_pass_fail_stats(df: DataFrame) -> DataFrame:
    """
    7. Pass/Fail Statistics.
    Counts overall passed (Marks >= 35) and failed (Marks < 35) students.
    """
    # Determine pass status for each row using when().otherwise()
    status_df = df.withColumn("Status", when(col("Marks") >= 35, "Pass").otherwise("Fail"))
    
    # Total count of all records
    total_records = status_df.count()
    
    if total_records == 0:
        total_records = 1  # Prevent division by zero error
        
    # Group by Status and count occurrences, calculating percentages dynamically
    return status_df.groupBy("Status") \
                    .agg(
                        count("Student_ID").alias("Count"),
                        F.round((count("Student_ID") / total_records) * 100, 2).alias("Percentage")
                    )

def get_attendance_analysis(df: DataFrame) -> DataFrame:
    """
    8. Attendance Analysis.
    Identifies students with critical attendance shortage (< 75%).
    Returns a dataframe of students with less than 75% attendance.
    """
    return df.filter(col("Attendance") < 75.0) \
             .select("Student_ID", "Student_Name", "Department", "Attendance") \
             .orderBy("Attendance")

def get_grade_distribution(df: DataFrame) -> DataFrame:
    """
    9. Grade Distribution.
    Counts how many students obtained each grade.
    """
    return df.groupBy("Grade") \
             .agg(count("Student_ID").alias("Student_Count")) \
             .orderBy(col("Student_Count").desc())

def get_student_ranking(df: DataFrame) -> DataFrame:
    """
    10. Student Ranking.
    Ranks students based on their overall average marks across all subjects.
    Demonstrates Window functions in PySpark.
    """
    # First, calculate the average marks for each student across all subjects they took
    student_avg_df = df.groupBy("Student_ID", "Student_Name", "Department") \
                       .agg(F.round(avg("Marks"), 2).alias("Overall_Average"))
                       
    # Define a Window specification ordered by Overall_Average descending
    # Window.orderBy specifies how the rows in our partition are sorted.
    windowSpec = Window.orderBy(col("Overall_Average").desc())
    
    # Apply the rank() window function over our windowSpec
    # rank() returns a rank starting from 1. If two students have same average, they get the same rank.
    return student_avg_df.withColumn("Rank", rank().over(windowSpec)) \
                          .orderBy("Rank")

def get_overall_class_stats(df: DataFrame) -> DataFrame:
    """
    11. Overall Class Statistics.
    Returns a summary statistics dataframe.
    """
    total_students = df.select(countDistinct("Student_ID")).first()[0]
    avg_marks = df.select(F.round(avg("Marks"), 2)).first()[0]
    max_marks = df.select(F.max("Marks")).first()[0]
    min_marks = df.select(F.min("Marks")).first()[0]
    avg_attendance = df.select(F.round(avg("Attendance"), 2)).first()[0]
    
    # Calculate Pass Percentage
    total_records = df.count()
    pass_records = df.filter(col("Marks") >= 35).count()
    pass_percentage = round((pass_records / total_records) * 100, 2) if total_records > 0 else 0.0
    
    # Return these metrics as a structured Spark DataFrame
    # To bypass Python 3.14 RDD pickling bugs, we build the DataFrame using pure Spark SQL union operations
    dummy_df = df.limit(1)
    
    stats_df = dummy_df.select(F.lit("Total Unique Students").alias("Metric"), F.lit(float(total_students)).alias("Value")) \
        .union(dummy_df.select(F.lit("Average Marks"), F.lit(float(avg_marks or 0)))) \
        .union(dummy_df.select(F.lit("Highest Mark Obtained"), F.lit(float(max_marks or 0)))) \
        .union(dummy_df.select(F.lit("Lowest Mark Obtained"), F.lit(float(min_marks or 0)))) \
        .union(dummy_df.select(F.lit("Average Attendance (%)"), F.lit(float(avg_attendance or 0)))) \
        .union(dummy_df.select(F.lit("Overall Pass Percentage (%)"), F.lit(float(pass_percentage))))
        
    return stats_df
