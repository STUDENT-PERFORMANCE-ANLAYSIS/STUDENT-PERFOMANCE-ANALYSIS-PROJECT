# src/cli.py
"""
Command Line Interface (CLI) for running offline PySpark Student Performance analysis.

Why this file exists:
If you do not want a web server or a browser dashboard, this script allows you to 
run the entire Apache Spark analytical pipeline directly from the terminal. 
It takes the path to a local CSV file on your computer, loads it into Spark, 
cleans it, runs all 11 analytics, prints a terminal summary, and saves the 
results directly into the 'output/' folder as standard CSV files.

How to run:
python src/cli.py
"""

import os
import sys
from pathlib import Path

# Setup project root and environment using utils module
project_root = Path(__file__).resolve().parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root / "src"))

# Import custom Spark pipeline modules
from utils import get_spark_session, setup_spark_environment
from data_loader import load_dataset, resolve_dataset_path
from data_cleaning import clean_student_data
from exporter import export_to_csv
import analytics

# Setup environment variables for PySpark
setup_spark_environment()

def run_offline_analysis(csv_filepath: str = None):
    """
    Runs the full PySpark cleaning and analysis pipeline on the given CSV path
    and exports all 11 analytical reports to the 'output/' folder.
    """
    resolved_path = resolve_dataset_path(csv_filepath)
    abs_path_str = str(resolved_path)

    if not resolved_path.exists():
        print(f"\nERROR: Dataset not found.")
        print(f"Expected location: {abs_path_str}")
        return

    print("\n========================================================")
    print("APACHE SPARK OFFLINE ANALYTICS RUNNER")
    print("========================================================")
    
    print("\n[Step 1/4] Initializing local Apache Spark JVM Session...")
    try:
        spark = get_spark_session("StudentPerformanceCLI")
        print("  --> Spark Session active.")
    except Exception as e:
        print(f"  --> [ERROR] Failed to start Spark: {e}")
        print("Please check Java installation and HADOOP_HOME paths.")
        return

    print(f"\n[Step 2/4] Reading dataset from: {abs_path_str}...")
    try:
        # Load raw dataset (validates columns and resolves mappings)
        raw_df = load_dataset(spark, abs_path_str)
        print(f"  --> Dataset loaded successfully. Columns resolved: {raw_df.columns}")
    except Exception as e:
        print(f"  --> [ERROR] Ingestion failed: {e}")
        return

    print("\n[Step 3/4] Cleaning data (removing duplicates, casting types, calculating grades)...")
    try:
        cleaned_df = clean_student_data(raw_df)
        total_records = cleaned_df.count()
        print(f"  --> Cleaning completed. Active records to analyze: {total_records}")
        
        # Cache in memory to speed up multiple analytical runs
        cleaned_df.cache()
    except Exception as e:
        print(f"  --> [ERROR] Data cleaning failed: {e}")
        return

    print("\n[Step 4/4] Running 11 Analytics & Writing reports to 'output/' directory...")
    
    # Dictionary mapping report names to Spark analytical operations
    queries = {
        "top_10_students": analytics.get_top_10_students,
        "highest_scorer": analytics.get_highest_scorer,
        "lowest_scorer": analytics.get_lowest_scorer,
        "subject_wise_averages": analytics.get_subject_wise_average,
        "department_wise_averages": analytics.get_department_wise_average,
        "semester_wise_performance": analytics.get_semester_performance,
        "pass_fail_statistics": analytics.get_pass_fail_stats,
        "attendance_shortage_list": analytics.get_attendance_analysis,
        "grade_distribution": analytics.get_grade_distribution,
        "student_rankings": analytics.get_student_ranking,
        "overall_class_statistics": analytics.get_overall_class_stats
    }
    
    output_dir = str(project_root / "output")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        
    for report_name, func in queries.items():
        try:
            print(f"  --> Executing: {report_name}...")
            # Run Spark transformation
            result_df = func(cleaned_df)
            
            # Export result to CSV using our exporter
            target_path = os.path.join(output_dir, f"{report_name}.csv")
            export_to_csv(result_df, target_path)
        except Exception as e:
            print(f"      [WARN] Failed to process {report_name}: {e}")
            
    print("\n========================================================")
    print(f"Success! All analytical reports exported to:")
    print(f"   {output_dir}")
    print("========================================================")

if __name__ == "__main__":
    print("\n" * 2)
    print("--- PySpark Student Performance Analytics ---")
    
    # Auto-detect local dataset files inside data/ or dataset/
    dataset_file = resolve_dataset_path()
    if dataset_file.exists():
        print(f"[INFO] Automatically detected local dataset: {dataset_file}")
        user_path = str(dataset_file)
    else:
        user_path = input("Enter the absolute file path to your CSV dataset: ").strip()
        user_path = user_path.replace('"', '').replace("'", "")
        
    run_offline_analysis(user_path)

