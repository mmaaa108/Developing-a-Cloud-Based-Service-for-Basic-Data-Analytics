"""
Configuration file for the backend application
Contains all application settings and parameters
"""

import os
from datetime import timedelta

class Config:
    """Application configuration"""
    
    # Flask Configuration
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    DEBUG = os.environ.get('DEBUG', 'True') == 'True'
    
    # File Upload Configuration
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER') or 'uploads'
    MAX_CONTENT_LENGTH = 500 * 1024 * 1024  # 500MB max file size
    ALLOWED_EXTENSIONS = {'csv', 'json', 'txt', 'pdf'}
    
    # Spark Configuration
    SPARK_MASTER = os.environ.get('SPARK_MASTER') or 'local[*]'
    SPARK_APP_NAME = 'CloudDataProcessing'
    
    # Cloud Storage Configuration (for AWS S3, GCP, Azure)
    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.environ.get('AWS_REGION') or 'us-east-1'
    S3_BUCKET = os.environ.get('S3_BUCKET')
    
    # GCP Configuration
    GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
    GCP_BUCKET = os.environ.get('GCP_BUCKET')
    GCP_CREDENTIALS_PATH = os.environ.get('GCP_CREDENTIALS_PATH')
    
    # Azure Configuration
    AZURE_STORAGE_CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    AZURE_CONTAINER = os.environ.get('AZURE_CONTAINER')
    
    # Job Configuration
    JOB_TIMEOUT = timedelta(hours=2)
    CLEANUP_OLD_JOBS_AFTER = timedelta(days=7)
    
    # Performance Configuration
    DEFAULT_WORKER_COUNTS = [1, 2, 4, 8]
    BASELINE_WORKERS = 1  # For speedup calculation
    
    # ML Configuration
    ML_TRAIN_TEST_SPLIT = 0.8
    ML_RANDOM_SEED = 42
    KMEANS_K = 3
    FPGROWTH_MIN_SUPPORT = 0.3
    FPGROWTH_MIN_CONFIDENCE = 0.6
    
    # Statistics Configuration
    SAMPLE_SIZE_FOR_STATS = 10000  # Sample size for large datasets
    
    # Logging Configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL') or 'INFO'
    LOG_FILE = os.environ.get('LOG_FILE') or 'app.log'
    
    @staticmethod
    def init_app(app):
        """Initialize application configuration"""
        pass

class DevelopmentConfig(Config):
    """Development environment configuration"""
    DEBUG = True
    SPARK_MASTER = 'local[*]'

class ProductionConfig(Config):
    """Production environment configuration"""
    DEBUG = False
    # In production, use actual Spark cluster
    SPARK_MASTER = os.environ.get('SPARK_MASTER') or 'spark://master:7077'

class TestingConfig(Config):
    """Testing environment configuration"""
    TESTING = True
    DEBUG = True
    SPARK_MASTER = 'local[2]'

# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}