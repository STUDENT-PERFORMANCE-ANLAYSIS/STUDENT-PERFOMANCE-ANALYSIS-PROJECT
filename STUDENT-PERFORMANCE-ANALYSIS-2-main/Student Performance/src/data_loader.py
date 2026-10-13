# src/data_loader.py
"""
Data Loader Module for reading and validating CSV files in Apache Spark.

Why this file exists:
In data engineering pipelines, ingestion is the first step. This module is responsible 
for loading raw CSV data into a PySpark DataFrame. Because student datasets can come 
from different colleges or systems, column names might differ slightly (e.g., 'Roll No' 
instead of 'Student_ID', or 'Score' instead of 'Marks'). This module inspects the headers 
dynamically, maps aliases to standard internal names, validates that mandatory columns 
exist, and adds missing optional columns with defaults. This makes our project robust 
and schema-agnostic.

Viva Tips:
- What is schema inference? In Spark, 'inferSchema=True' triggers an extra scan of the 
  dataset to guess data types (Integer, String, etc.) instead of reading everything as String.
- What is the difference between Transformation and Action in Spark? 
  - Transformations (like withColumn, withColumnRenamed) are lazy; they define the plan but do not execute it.
  - Actions (like count, show, collect) trigger the actual execution.
- What is 'lit()'? It stands for 'literal' and is a Spark SQL function used to add a constant/literal value as a column.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

# Dictionary of standard internal column names mapped to possible variations/aliases
COLUMN_ALIASES = {
    "Student_ID": ["student_id", "studentid", "student id", "roll_no", "roll no", "rollno", "id"],
    "Student_Name": ["student_name", "studentname", "student name", "name", "full_name", "fullname"],
    "Department": ["department", "dept", "branch", "stream"],
    "Semester": ["semester", "sem", "term"],
    "Subject": ["subject", "sub", "course", "paper"],
    "Marks": ["marks", "mark", "score", "total_marks", "total marks", "total", "total_score"],
    "Attendance": ["attendance", "attendance%", "attendance (%)", "att", "present_percentage"],
    "Grade": ["grade", "letter_grade", "letter grade", "class_grade"]
}

# Define which fields are absolutely required for basic processing
MANDATORY_COLUMNS = ["Student_ID", "Subject", "Marks"]

def detect_and_map_schema(df: DataFrame) -> DataFrame:
    """
    Inspects the columns of the input DataFrame and renames them to standard names
    if they match any alias. It warns/raises errors for missing mandatory columns
    and adds missing optional columns with default values.
    
    Parameters:
    df (DataFrame): Raw PySpark DataFrame read from CSV.
    
    Returns:
    DataFrame: Renamed and aligned DataFrame.
    """
    original_cols = df.columns
    mapped_columns = {}
    
    # Iterate through our standard expected columns and check for matches in the dataset
    for standard_name, aliases in COLUMN_ALIASES.items():
        for original_col in original_cols:
            cleaned_original = original_col.strip().lower().replace("_", " ").replace("-", " ")
            if cleaned_original == standard_name.lower().replace("_", " ") or cleaned_original in [a.lower().replace("_", " ") for a in aliases]:
                mapped_columns[original_col] = standard_name
                break # Found a match, move to next standard column
                
    # Apply column renames to the DataFrame
    # Note: withColumnRenamed is a Spark TRANSFORMATION. It returns a new DataFrame.
    for original, standard in mapped_columns.items():
        df = df.withColumnRenamed(original, standard)
        
    # Check if all mandatory columns were successfully mapped
    resolved_cols = df.columns
    missing_mandatory = [col for col in MANDATORY_COLUMNS if col not in resolved_cols]
    
    if missing_mandatory:
        raise ValueError(
            f"Missing required columns in dataset: {missing_mandatory}. "
            f"Please ensure your dataset contains equivalent fields for: {MANDATORY_COLUMNS}"
        )
        
    # Inject optional columns with default null/lit values if they don't exist in the CSV
    # This prevents the downstream code from crashing when these columns are referenced.
    # lit() is a Spark Transformation that creates a column with a constant literal value.
    if "Student_Name" not in resolved_cols:
        df = df.withColumn("Student_Name", lit("Unknown"))
    if "Department" not in resolved_cols:
        df = df.withColumn("Department", lit("General"))
    if "Semester" not in resolved_cols:
        df = df.withColumn("Semester", lit("All Semesters"))
    if "Attendance" not in resolved_cols:
        df = df.withColumn("Attendance", lit(None).cast("double"))
    if "Grade" not in resolved_cols:
        df = df.withColumn("Grade", lit(None).cast("string"))
        
    # Rearrange and select only the standard columns to keep the dataset clean
    standard_fields = list(COLUMN_ALIASES.keys())
    df = df.select(*standard_fields)
    
    return df

import os
from pathlib import Path

def resolve_dataset_path(file_path: str = None) -> Path:
    """
    Resolves dataset path dynamically relative to the project root.
    Checks data/students_10000.csv first, then dataset/student.csv, dataset/students.csv.
    """
    project_root = Path(__file__).resolve().parent.parent

    if file_path:
        p = Path(file_path)
        if not p.is_absolute():
            p = (project_root / p).resolve()
        return p

    # Standard locations
    candidates = [
        project_root / "data" / "students_10000.csv",
        project_root / "dataset" / "student.csv",
        project_root / "dataset" / "students.csv",
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()

    # Default expected path if none exist yet
    return (project_root / "data" / "students_10000.csv").resolve()

def load_dataset(spark, file_path: str = None) -> DataFrame:
    """
    Loads a CSV file from a path, infers its schema, maps columns, and validates it.
    Performs dataset validation step and prints terminal details as required.
    
    Parameters:
    spark (SparkSession): The active Spark Session.
    file_path (str, optional): Path to the CSV file. If None, resolves to default data location.
    
    Returns:
    DataFrame: Mapped and validated Spark DataFrame.
    """
    resolved_path = resolve_dataset_path(file_path)
    abs_path_str = str(resolved_path)

    if not resolved_path.exists():
        print(f"\nERROR: Dataset not found.")
        print(f"Expected location: {abs_path_str}")
        raise FileNotFoundError(f"ERROR: Dataset not found at expected location: {abs_path_str}")

    file_size = os.path.getsize(abs_path_str)

    print(f"\nDataset path: {abs_path_str}")
    print(f"Dataset exists: True")
    print(f"Dataset size: {file_size} bytes")
    print("Loading dataset with Spark...")

    # Read CSV using Spark DataFrameReader API
    raw_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "FAILFAST") \
        .csv(abs_path_str)

    # Map and validate schemas dynamically
    processed_df = detect_and_map_schema(raw_df)
    record_count = processed_df.count()

    print("Dataset loaded successfully.")
    print(f"Number of students: {record_count}")

    return processed_df

