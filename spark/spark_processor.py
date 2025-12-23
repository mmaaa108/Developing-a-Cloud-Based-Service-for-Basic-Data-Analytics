"""
Spark Processor Module
Handles all Spark-related operations including data loading and job coordination
"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import time
import os
from statistics import Statistics
from ml_jobs import MLJobs

class SparkProcessor:
    """Main Spark processor class"""
    
    def __init__(self, master='local[*]'):
        """Initialize Spark processor"""
        self.master = master
        self.baseline_time = {}  # Store baseline execution times
    
    def _create_spark_session(self, app_name, worker_count=1):
        """
        Create a Spark session with specified configuration
        
        Args:
            app_name: Name of the Spark application
            worker_count: Number of workers to use
            
        Returns:
            SparkSession object
        """
        conf = SparkConf()
        conf.setAppName(app_name)
        conf.setMaster(self.master)
        
        # Configure based on worker count
        if 'local' in self.master:
            # For local mode, set number of cores
            conf.setMaster(f'local[{worker_count}]')
        else:
            # For cluster mode, set executor instances
            conf.set('spark.executor.instances', str(worker_count))
            conf.set('spark.executor.cores', '4')
            conf.set('spark.executor.memory', '4g')
        
        # Additional performance configurations
        conf.set('spark.sql.shuffle.partitions', str(worker_count * 4))
        conf.set('spark.default.parallelism', str(worker_count * 4))
        
        # Create session
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        
        return spark
    
    def load_data(self, filepath, spark):
        """
        Load data from file based on file type
        
        Args:
            filepath: Path to the data file
            spark: SparkSession object
            
        Returns:
            Spark DataFrame
        """
        file_ext = os.path.splitext(filepath)[1].lower()
        
        if file_ext == '.csv':
            df = spark.read.csv(filepath, header=True, inferSchema=True)
        elif file_ext == '.json':
            df = spark.read.json(filepath)
        elif file_ext == '.txt':
            # Read as text and create DataFrame
            df = spark.read.text(filepath)
        elif file_ext == '.parquet':
            df = spark.read.parquet(filepath)
        else:
            raise ValueError(f"Unsupported file type: {file_ext}")
        
        return df
    
    def compute_statistics(self, filepath, stat_type, worker_count=1):
        """
        Compute descriptive statistics
        
        Args:
            filepath: Path to data file
            stat_type: Type of statistics to compute
            worker_count: Number of workers
            
        Returns:
            Dictionary with statistical results
        """
        app_name = f'Statistics_{stat_type}_{worker_count}workers'
        spark = self._create_spark_session(app_name, worker_count)
        
        try:
            # Load data
            df = self.load_data(filepath, spark)
            
            # Compute statistics based on type
            stats_computer = Statistics(spark)
            
            if stat_type == 'basic_info':
                result = stats_computer.basic_info(df)
            elif stat_type == 'summary_stats':
                result = stats_computer.summary_statistics(df)
            elif stat_type == 'missing_values':
                result = stats_computer.missing_values(df)
            elif stat_type == 'unique_values':
                result = stats_computer.unique_values(df)
            else:
                raise ValueError(f"Unknown statistics type: {stat_type}")
            
            return result
            
        finally:
            spark.stop()
    
    def run_ml_job(self, filepath, job_type, worker_count=1):
        """
        Run machine learning job
        
        Args:
            filepath: Path to data file
            job_type: Type of ML job
            worker_count: Number of workers
            
        Returns:
            Dictionary with ML results and performance metrics
        """
        app_name = f'ML_{job_type}_{worker_count}workers'
        spark = self._create_spark_session(app_name, worker_count)
        
        try:
            # Load data
            df = self.load_data(filepath, spark)
            
            # Measure execution time
            start_time = time.time()
            
            # Run ML job
            ml_processor = MLJobs(spark)
            
            if job_type == 'linear_regression':
                result = ml_processor.linear_regression(df)
            elif job_type == 'kmeans':
                result = ml_processor.kmeans_clustering(df)
            elif job_type == 'decision_tree':
                result = ml_processor.decision_tree(df)
            elif job_type == 'fpgrowth':
                result = ml_processor.fp_growth(df)
            else:
                raise ValueError(f"Unknown ML job type: {job_type}")
            
            execution_time = time.time() - start_time
            
            # Calculate speedup and efficiency
            job_key = f"{filepath}_{job_type}"
            
            if worker_count == 1:
                # Store baseline time
                self.baseline_time[job_key] = execution_time
                speedup = 1.0
                efficiency = 100.0
            else:
                # Calculate speedup relative to baseline
                baseline = self.baseline_time.get(job_key, execution_time)
                speedup = baseline / execution_time if execution_time > 0 else 1.0
                efficiency = (speedup / worker_count) * 100
            
            return {
                'result': result,
                'execution_time': execution_time,
                'speedup': speedup,
                'efficiency': efficiency
            }
            
        finally:
            spark.stop()
    
    def cleanup(self):
        """Cleanup resources"""
        self.baseline_time.clear()