# Cloud-Based Distributed Data Processing Service

A comprehensive web application for distributed data processing and machine learning analytics using Apache Spark/PySpark.

##  Features

### Data Upload
- Support for multiple file formats: CSV, JSON, TXT, PDF
- Drag-and-drop interface
- File size validation (up to 500MB)
- File type validation

### Descriptive Statistics
1. **Basic Information**: Data dimensions, types, size estimation
2. **Summary Statistics**: Min, max, mean, standard deviation
3. **Missing Values Analysis**: Null counts and percentages
4. **Unique Values**: Distinct value counts and ratios

### Machine Learning Jobs
1. **Linear Regression**: Predictive modeling with RMSE and R² metrics
2. **K-Means Clustering**: Unsupervised clustering with configurable k
3. **Decision Tree Classification**: Tree-based classification
4. **FP-Growth**: Frequent pattern mining and association rules

### Performance Analysis
- Execution on 1, 2, 4, and 8 workers
- Speedup calculation
- Efficiency measurement
- Scalability assessment

##  Architecture

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│   Frontend  │────────▶│   Backend   │────────▶│    Spark    │
│  (HTML/CSS/ │         │   (Flask)   │         │  Processing │
│     JS)     │◀────────│             │◀────────│             │
└─────────────┘         └─────────────┘         └─────────────┘
                               │
                               ▼
                        ┌─────────────┐
                        │   Storage   │
                        │(Local/Cloud)│
                        └─────────────┘
```

## 📋 Prerequisites

- Python 3.8 or higher
- Apache Spark 3.5.0
- Java 8 or higher (for Spark)
- Modern web browser

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/cloud-data-processing.git
cd cloud-data-processing
```

### 2. Install Dependencies

```bash
pip install -r backend/requirements.txt
```

### 3. Set Up Environment Variables

Create a `.env` file in the backend directory:

```bash
# Flask Configuration
SECRET_KEY=your-secret-key-here
DEBUG=True

# Spark Configuration
SPARK_MASTER=local[*]

# Upload Configuration
UPLOAD_FOLDER=uploads
MAX_CONTENT_LENGTH=524288000

# Cloud Storage (Optional)
# AWS_ACCESS_KEY_ID=your-aws-key
# AWS_SECRET_ACCESS_KEY=your-aws-secret
# S3_BUCKET=your-bucket-name
```

### 4. Install Apache Spark

#### Option A: Using pip (Included with PySpark)
Already installed with requirements.txt

#### Option B: Manual Installation
Download from: https://spark.apache.org/downloads.html

```bash
# Extract Spark
tar -xzf spark-3.5.0-bin-hadoop3.tgz
export SPARK_HOME=/path/to/spark-3.5.0-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin
```

##  Usage

### 1. Start the Backend Server

```bash
cd backend
python app.py
```

The server will start on `http://localhost:5000`

### 2. Open the Frontend

Open `frontend/index.html` in your web browser, or serve it using a simple HTTP server:

```bash
cd frontend
python -m http.server 8000
```

Then navigate to `http://localhost:8000`

### 3. Upload and Process Data

1. **Upload Dataset**: Click or drag-and-drop your file
2. **Select Options**: Choose statistics and ML jobs
3. **Configure Workers**: Select cluster configurations
4. **Start Processing**: Click "Start Processing"
5. **View Results**: Explore statistics, ML results, and performance metrics
6. **Download Report**: Get formatted results in a text file

##  Sample Datasets

You can test the application with datasets from:

- [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/index.php)
- [Kaggle Datasets](https://www.kaggle.com/datasets)
- [Google Dataset Search](https://datasetsearch.research.google.com/)

Recommended datasets:
- Iris Dataset (Classification)
- Boston Housing (Regression)
- Mall Customers (Clustering)
- Market Basket (Association Rules)

##  Configuration

### Spark Configuration

Modify `backend/config.py` to adjust Spark settings:

```python
# For local mode
SPARK_MASTER = 'local[*]'

# For standalone cluster
SPARK_MASTER = 'spark://master:7077'

# For YARN
SPARK_MASTER = 'yarn'
```

### ML Parameters

Adjust ML parameters in `backend/config.py`:

```python
ML_TRAIN_TEST_SPLIT = 0.8
KMEANS_K = 3
FPGROWTH_MIN_SUPPORT = 0.3
FPGROWTH_MIN_CONFIDENCE = 0.6
```

## 📁 Project Structure

```
cloud-data-processing/
├── frontend/
│   ├── index.html          # Main UI
│   ├── styles.css          # Styling
│   └── app.js              # Frontend logic
├── backend/
│   ├── app.py              # Flask application
│   ├── config.py           # Configuration
│   └── requirements.txt    # Python dependencies
├── spark/
│   ├── spark_processor.py  # Spark coordination
│   ├── statistics.py       # Statistics computations
│   └── ml_jobs.py          # ML implementations
├── utils/
│   ├── storage_handler.py  # File storage
│   └── performance_tracker.py  # Performance metrics
└── README.md
```

##  Deployment

### AWS Deployment

1. **Set up EC2 instance**
2. **Install Spark on EMR**
3. **Configure S3 for storage**
4. **Deploy Flask app on Elastic Beanstalk**

### GCP Deployment

1. **Use Cloud Dataproc for Spark**
2. **Store data in Cloud Storage**
3. **Deploy Flask on App Engine**

### Azure Deployment

1. **Use HDInsight for Spark**
2. **Store data in Blob Storage**
3. **Deploy Flask on App Service**

##  Performance Metrics

The application tracks:

- **Execution Time**: Time taken for each job
- **Speedup**: Performance gain with multiple workers
  - Formula: `Speedup = T₁ / Tₙ`
- **Efficiency**: How well resources are utilized
  - Formula: `Efficiency = (Speedup / n) × 100%`

### Scalability Assessment

- **Excellent (≥85% efficiency)**: Near-linear scalability
- **Good (70-85%)**: Effective parallelization
- **Fair (50-70%)**: Moderate scalability
- **Poor (<50%)**: Limited benefits

##  Troubleshooting

### Common Issues

**1. Spark Not Found**
```bash
# Set SPARK_HOME
export SPARK_HOME=/path/to/spark
export PATH=$PATH:$SPARK_HOME/bin
```

**2. Out of Memory**
```python
# Increase Spark memory in config.py
conf.set('spark.executor.memory', '8g')
conf.set('spark.driver.memory', '4g')
```

**3. Port Already in Use**
```bash
# Change port in app.py
app.run(port=5001)
```

**4. CORS Errors**
```python
# Ensure CORS is properly configured
from flask_cors import CORS
CORS(app)
```

##  API Documentation

### Upload Endpoint

```http
POST /api/upload
Content-Type: multipart/form-data

Parameters:
- file: File to upload
- statistics: JSON array of statistic types
- ml_jobs: JSON array of ML job types
- workers: JSON array of worker counts

Response:
{
  "job_id": "uuid",
  "message": "File uploaded successfully",
  "status": "processing"
}
```

### Status Endpoint

```http
GET /api/status/:job_id

Response:
{
  "job_id": "uuid",
  "status": "processing|completed|failed",
  "progress": 75,
  "message": "Running ML jobs...",
  "jobs": [...]
}
```

### Results Endpoint

```http
GET /api/results/:job_id

Response:
{
  "statistics": {...},
  "ml_results": {...},
  "performance": [...]
}
```

##  Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

##  License

This project is licensed under the MIT License.

## 👥 Authors

- Mahmoud Alkhudari - Initial work

##  Acknowledgments

- Apache Spark community
- UCI Machine Learning Repository
- Flask documentation
- PySpark documentation

##  Contact

For questions or support, please contact: alkhodaryabed90@gma.com


##  Results Format

The downloaded results are formatted as a structured text report with three main sections:

### 1. Descriptive Statistics
Shows all selected statistical analyses with clear headers and formatted data

### 2. Machine Learning Results
Displays results for each ML algorithm with:
- Model parameters
- Training/test metrics
- Performance indicators

### 3. Performance Analysis
Presents scalability metrics in a table format:
- Workers count
- Execution time
- Speedup factor
- Efficiency percentage

 
