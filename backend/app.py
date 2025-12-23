"""
Backend API for Cloud-Based Data Processing Service
Handles file uploads, job management, and Spark processing coordination
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import sys
import uuid
import json
import threading
from datetime import datetime
from werkzeug.utils import secure_filename

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import custom modules
from config import Config
from utils.storage_handler import StorageHandler

# Lazy import SparkProcessor to avoid PySpark compatibility issues
try:
    from spark.spark_processor import SparkProcessor
    SPARK_AVAILABLE = True
except Exception as e:
    print(f"Warning: Spark not available: {e}")
    SparkProcessor = None
    SPARK_AVAILABLE = False

# Initialize Flask app
app = Flask(__name__)
app.config.from_object(Config)
CORS(app)

# Initialize handlers
storage_handler = StorageHandler(app.config['UPLOAD_FOLDER'])
spark_processor = SparkProcessor(app.config['SPARK_MASTER']) if SPARK_AVAILABLE else None

# Job storage (in production, use a database)
jobs = {}

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """
    Handle file upload and initialize processing job
    
    Expected form data:
    - file: uploaded file
    - statistics: JSON array of selected statistics
    - ml_jobs: JSON array of selected ML jobs
    - workers: JSON array of worker counts
    """
    try:
        # Validate file
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # Validate file type
        if not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type'}), 400
        
        # Get processing options
        try:
            statistics = json.loads(request.form.get('statistics', '[]'))
            ml_jobs = json.loads(request.form.get('ml_jobs', '[]'))
            workers = json.loads(request.form.get('workers', '[]'))
        except json.JSONDecodeError:
            return jsonify({'error': 'Invalid options format'}), 400
        
        # Validate options
        if not statistics or not ml_jobs or not workers:
            return jsonify({'error': 'Please select processing options'}), 400
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Save file
        filename = secure_filename(file.filename)
        filepath = storage_handler.save_file(file, job_id, filename)
        
        # Create job record
        jobs[job_id] = {
            'id': job_id,
            'filename': filename,
            'filepath': filepath,
            'status': 'initialized',
            'progress': 0,
            'message': 'Job initialized',
            'statistics': statistics,
            'ml_jobs': ml_jobs,
            'workers': workers,
            'created_at': datetime.now().isoformat(),
            'jobs': [],
            'results': None,
            'error': None
        }
        
        # Start processing in background thread
        thread = threading.Thread(
            target=process_job,
            args=(job_id,)
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'job_id': job_id,
            'message': 'File uploaded successfully',
            'status': 'processing'
        }), 200
        
    except Exception as e:
        app.logger.error(f"Upload error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/status/<job_id>', methods=['GET'])
def get_status(job_id):
    """Get job status"""
    if job_id not in jobs:
        return jsonify({'error': 'Job not found'}), 404
    
    job = jobs[job_id]
    return jsonify({
        'job_id': job_id,
        'status': job['status'],
        'progress': job['progress'],
        'message': job['message'],
        'jobs': job['jobs'],
        'error': job['error']
    })

@app.route('/api/results/<job_id>', methods=['GET'])
def get_results(job_id):
    """Get job results"""
    if job_id not in jobs:
        return jsonify({'error': 'Job not found'}), 404
    
    job = jobs[job_id]
    
    if job['status'] != 'completed':
        return jsonify({'error': 'Job not completed'}), 400
    
    return jsonify(job['results'])

def process_job(job_id):
    """
    Process job in background
    Coordinates Spark processing and updates job status
    """
    try:
        if not SPARK_AVAILABLE:
            raise Exception("Spark is not available. Please install compatible PySpark version for Python 3.13")
        
        job = jobs[job_id]
        job['status'] = 'processing'
        job['progress'] = 10
        job['message'] = 'Loading data...'
        
        # Initialize results structure
        results = {
            'statistics': {},
            'ml_results': {},
            'performance': []
        }
        
        # Load data
        filepath = job['filepath']
        
        # Process statistics
        job['progress'] = 20
        job['message'] = 'Computing statistics...'
        
        for stat in job['statistics']:
            job['jobs'].append({
                'name': f'Statistics: {stat}',
                'status': 'running'
            })
            
            stat_result = spark_processor.compute_statistics(
                filepath,
                stat,
                job['workers'][0]  # Use first worker config for stats
            )
            
            results['statistics'][stat] = stat_result
            
            # Update job status
            job['jobs'][-1]['status'] = 'completed'
            job['progress'] = 20 + (30 * len(results['statistics']) / len(job['statistics']))
        
        # Process ML jobs
        job['message'] = 'Running machine learning jobs...'
        
        ml_progress_start = 50
        ml_progress_range = 40
        
        for i, ml_job in enumerate(job['ml_jobs']):
            # Run on different worker configurations
            job_results = {}
            
            for worker_count in job['workers']:
                job['jobs'].append({
                    'name': f'{ml_job} ({worker_count} workers)',
                    'status': 'running'
                })
                
                ml_result = spark_processor.run_ml_job(
                    filepath,
                    ml_job,
                    worker_count
                )
                
                # Store result
                if worker_count not in job_results:
                    job_results[worker_count] = ml_result['result']
                
                # Store performance metrics
                results['performance'].append({
                    'job': ml_job,
                    'workers': worker_count,
                    'execution_time': ml_result['execution_time'],
                    'speedup': ml_result['speedup'],
                    'efficiency': ml_result['efficiency']
                })
                
                job['jobs'][-1]['status'] = 'completed'
            
            # Store ML results (use results from largest cluster)
            results['ml_results'][ml_job] = job_results[max(job['workers'])]
            
            # Update progress
            progress = ml_progress_start + (ml_progress_range * (i + 1) / len(job['ml_jobs']))
            job['progress'] = progress
        
        # Calculate average performance per worker configuration
        performance_summary = []
        for worker_count in job['workers']:
            worker_metrics = [p for p in results['performance'] if p['workers'] == worker_count]
            avg_time = sum(m['execution_time'] for m in worker_metrics) / len(worker_metrics)
            avg_speedup = sum(m['speedup'] for m in worker_metrics) / len(worker_metrics)
            avg_efficiency = sum(m['efficiency'] for m in worker_metrics) / len(worker_metrics)
            
            performance_summary.append({
                'workers': worker_count,
                'execution_time': avg_time,
                'speedup': avg_speedup,
                'efficiency': avg_efficiency
            })
        
        results['performance'] = performance_summary
        
        # Complete job
        job['status'] = 'completed'
        job['progress'] = 100
        job['message'] = 'Processing completed successfully'
        job['results'] = results
        
    except Exception as e:
        app.logger.error(f"Processing error for job {job_id}: {str(e)}")
        job['status'] = 'failed'
        job['error'] = str(e)
        job['message'] = 'Processing failed'

def allowed_file(filename):
    """Check if file type is allowed"""
    ALLOWED_EXTENSIONS = {'csv', 'json', 'txt', 'pdf'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

if __name__ == '__main__':
    # Create upload folder if not exists
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    # Run app
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True
    )