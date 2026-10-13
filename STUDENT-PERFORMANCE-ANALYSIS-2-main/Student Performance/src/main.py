# src/main.py
"""
Flask Backend application serving as the project coordinator and REST controller.

Why this file exists:
Apache Spark is a data processing library, not a web server. To expose our Spark 
analysis as an interactive web app, we build a Flask application. This script acts 
as the server. It handles file uploads, routes API requests from the frontend, coordinates 
data ingestion, cleaning, and analytics, and serializes Spark DataFrame outputs into 
JSON payloads that the browser can display.

Communication flow:
1. User uploads a CSV. Flask saves the file, invokes 'utils.py' to get/start a SparkSession, 
   'data_loader.py' to read/map headers, and 'data_cleaning.py' to clean it.
2. The cleaned DataFrame is cached globally in memory.
3. When the user clicks an analysis button, JavaScript fetches the specific '/analyze/<query>' route.
4. Flask executes the corresponding function in 'analytics.py', converts the output into Python 
   dictionaries, and returns it as a JSON API response.
5. If the user clicks export, Flask runs 'exporter.py' to save the report as a single CSV.

Viva Tips:
- How does Flask serve static assets? By setting `static_folder` pointing to the 'ui' directory, 
  Flask can serve files like `style.css` and `script.js` directly to the client.
- Why is the DataFrame cached in a global variable? So we don't reload and re-clean the CSV file 
  from disk for every click, which saves significant computing overhead.
- What does `.collect()` do? It is a Spark ACTION that pulls the distributed results from worker 
  nodes and returns them as a local list to the driver (Flask server memory).
"""

import os
import sys
import threading
from pathlib import Path

# Setup project root and environment using utils module
project_root = Path(__file__).resolve().parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root / "src"))

from flask import Flask, request, jsonify
from flask_cors import CORS

# Import custom Spark components
from utils import get_spark_session, setup_spark_environment
from data_loader import load_dataset, resolve_dataset_path
from data_cleaning import clean_student_data
import analytics
from exporter import export_to_csv

# Setup environment variables for PySpark
setup_spark_environment()

# Setup Flask application (serves UI assets from root and /ui paths)
app = Flask(
    __name__, 
    static_folder=str(project_root / 'ui'),
    static_url_path=''
)
CORS(app, resources={r"/*": {"origins": "*"}})


# Global variables for SparkSession, processed DataFrame, and pre-computed response cache
SPARK = None
CLEANED_DF = None
ANALYTICS_CACHE = {}

# Folder for holding uploaded dataset files
UPLOAD_FOLDER = str(project_root / 'data')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Mapping of route query names to their corresponding analytical functions
ANALYTICS_ROUTING = {
    'top10': analytics.get_top_10_students,
    'highest': analytics.get_highest_scorer,
    'lowest': analytics.get_lowest_scorer,
    'subject_avg': analytics.get_subject_wise_average,
    'dept_avg': analytics.get_department_wise_average,
    'sem_perf': analytics.get_semester_performance,
    'pass_fail': analytics.get_pass_fail_stats,
    'attendance': analytics.get_attendance_analysis,
    'grade_dist': analytics.get_grade_distribution,
    'ranking': analytics.get_student_ranking,
    'class_stats': analytics.get_overall_class_stats
}

def warm_analytics_cache():
    """
    Pre-computes all analytical queries in memory.
    Subsequent clicks on any report take 0ms!
    """
    global CLEANED_DF, ANALYTICS_CACHE
    if CLEANED_DF is None:
        return
    
    # 1. Pre-cache preview
    if 'preview' not in ANALYTICS_CACHE:
        try:
            preview_rows = CLEANED_DF.limit(15).collect()
            ANALYTICS_CACHE['preview'] = {
                "columns": CLEANED_DF.columns,
                "data": [row.asDict() for row in preview_rows]
            }
        except Exception as e:
            print(f"[CACHE WARN] Preview cache error: {e}")

    # 2. Pre-cache all 11 reports
    for q_name, q_func in ANALYTICS_ROUTING.items():
        if q_name not in ANALYTICS_CACHE:
            try:
                res_df = q_func(CLEANED_DF)
                rows = res_df.collect()
                ANALYTICS_CACHE[q_name] = {
                    "columns": res_df.columns,
                    "data": [row.asDict() for row in rows]
                }
            except Exception as e:
                print(f"[CACHE WARN] Error caching {q_name}: {e}")

    print("[CACHE READY] All analytical queries pre-computed in memory! Sub-millisecond responses enabled.")

@app.route('/')
def index():
    """
    Serves the main HTML5 dashboard layout page.
    """
    return app.send_static_file('index.html')

@app.route('/ui/<path:filename>')
def serve_ui_subpath(filename):
    """
    Provides backward compatibility for requests referencing /ui/<filename>.
    """
    return app.send_static_file(filename)


@app.route('/status', methods=['GET'])
def get_status():
    """
    Endpoint checking if a dataset is already loaded in Spark Session memory,
    or auto-loads the permanent dataset (data/students_10000.csv or dataset/student.csv).
    """
    global SPARK, CLEANED_DF, ANALYTICS_CACHE
    
    if CLEANED_DF is not None:
        if len(ANALYTICS_CACHE) == 0:
            threading.Thread(target=warm_analytics_cache, daemon=True).start()
        return jsonify({
            "status": "ready",
            "filename": "Cached Dataset",
            "columns": CLEANED_DF.columns,
            "count": CLEANED_DF.count()
        })
        
    try:
        found_file = resolve_dataset_path()
        if found_file.exists():
            if SPARK is None:
                SPARK = get_spark_session()
            raw_df = load_dataset(SPARK, str(found_file))
            CLEANED_DF = clean_student_data(raw_df)
            CLEANED_DF.cache()
            threading.Thread(target=warm_analytics_cache, daemon=True).start()
            return jsonify({
                "status": "ready",
                "filename": found_file.name,
                "columns": CLEANED_DF.columns,
                "count": CLEANED_DF.count()
            })
    except Exception as e:
        return jsonify({"status": "no_data", "message": f"Auto-load failed: {str(e)}"})
            
    return jsonify({"status": "no_data", "message": "No dataset loaded. Expected location: data/students_10000.csv"})


@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Endpoint to receive and process a uploaded student dataset.
    """
    global SPARK, CLEANED_DF, ANALYTICS_CACHE
    
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded in the request."}), 400
        
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file."}), 400
        
    if not file.filename.endswith('.csv'):
        return jsonify({"error": "Invalid file format. Please upload a CSV file."}), 400
        
    # Save the file locally to the dataset folder
    file_path = os.path.join(UPLOAD_FOLDER, 'students.csv')
    try:
        file.save(file_path)
    except Exception as e:
        return jsonify({"error": f"Failed to save uploaded file: {str(e)}"}), 500
        
    # Execute Spark ingestion and cleaning pipeline
    try:
        # Initialize Spark Session if it doesn't exist yet
        if SPARK is None:
            SPARK = get_spark_session()
            
        # Ingest CSV and map schemas dynamically
        raw_df = load_dataset(SPARK, file_path)
        
        # Clean duplicates, null values, and apply logic
        CLEANED_DF = clean_student_data(raw_df)
        
        # Cache the cleaned DataFrame in memory to optimize subsequent queries
        CLEANED_DF.cache()
        ANALYTICS_CACHE.clear()
        threading.Thread(target=warm_analytics_cache, daemon=True).start()
        
        # Return information about the loaded dataset
        return jsonify({
            "status": "success",
            "filename": file.filename,
            "columns": CLEANED_DF.columns,
            "count": CLEANED_DF.count()
        })
        
    except Exception as e:
        CLEANED_DF = None
        ANALYTICS_CACHE.clear()
        return jsonify({"error": f"Spark Pipeline Initialization Error: {str(e)}"}), 500

@app.route('/preview', methods=['GET'])
def get_preview():
    """
    Endpoint that returns a preview of the first 15 records in the active dataset.
    Uses in-memory cache for instant responses.
    """
    global CLEANED_DF, ANALYTICS_CACHE
    if CLEANED_DF is None:
        return jsonify({"error": "No active dataset loaded in Spark."}), 400
        
    if 'preview' in ANALYTICS_CACHE:
        return jsonify(ANALYTICS_CACHE['preview'])
        
    try:
        preview_rows = CLEANED_DF.limit(15).collect()
        payload = {
            "columns": CLEANED_DF.columns,
            "data": [row.asDict() for row in preview_rows]
        }
        ANALYTICS_CACHE['preview'] = payload
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": f"Failed to generate preview: {str(e)}"}), 500

@app.route('/analyze/<query_type>', methods=['GET'])
def analyze(query_type):
    """
    Endpoint to execute specific analytical queries.
    Uses in-memory cache for sub-millisecond tab switching.
    """
    global CLEANED_DF, ANALYTICS_CACHE
    if CLEANED_DF is None:
        return jsonify({"error": "No active dataset loaded. Please upload a CSV first."}), 400
        
    if query_type not in ANALYTICS_ROUTING:
        return jsonify({"error": f"Unknown query type: {query_type}"}), 404
        
    # Instant cache return (0ms)
    if query_type in ANALYTICS_CACHE:
        return jsonify(ANALYTICS_CACHE[query_type])
        
    try:
        analysis_func = ANALYTICS_ROUTING[query_type]
        result_df = analysis_func(CLEANED_DF)
        rows = result_df.collect()
        payload = {
            "columns": result_df.columns,
            "data": [row.asDict() for row in rows]
        }
        ANALYTICS_CACHE[query_type] = payload
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": f"Spark execution failed: {str(e)}"}), 500

@app.route('/export/<query_type>', methods=['GET'])
def export(query_type):
    """
    Endpoint that triggers query calculations and writes output directly to CSV file.
    """
    global CLEANED_DF
    if CLEANED_DF is None:
        return jsonify({"error": "No active dataset loaded."}), 400
        
    if query_type not in ANALYTICS_ROUTING:
        return jsonify({"error": f"Unknown query type: {query_type}"}), 404
        
    try:
        analysis_func = ANALYTICS_ROUTING[query_type]
        result_df = analysis_func(CLEANED_DF)
        
        # Define path inside local output folder
        target_filename = f"{query_type}_report.csv"
        target_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../output', target_filename))
        
        # Export as a single CSV
        export_to_csv(result_df, target_path)
        
        return jsonify({
            "status": "success",
            "path": target_path
        })
    except Exception as e:
        return jsonify({"error": f"Export process failed: {str(e)}"}), 500

@app.after_request
def add_cors_headers(response):
    """
    Appends CORS headers to every response. This allows frontend files served 
    by static web servers (like port 61547 / VS Code Live Server) to successfully 
    fetch API results from the Flask Spark backend on port 5000.
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
    response.headers['Access-Control-Allow-Methods'] = 'GET,PUT,POST,DELETE,OPTIONS'
    return response

if __name__ == '__main__':
    # Start the Flask development server on localhost:5000
    # debug=False is recommended when running Spark applications in same process
    # to avoid thread-safety restart loops.
    app.run(host='127.0.0.1', port=5000, debug=False)
