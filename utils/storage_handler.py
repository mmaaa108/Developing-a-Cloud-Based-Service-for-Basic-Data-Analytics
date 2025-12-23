"""
Storage Handler Module
Handles file storage operations (local and cloud storage)
"""

import os
import shutil
from pathlib import Path
from datetime import datetime

class StorageHandler:
    """Handles file storage operations"""
    
    def __init__(self, base_path='uploads'):
        """
        Initialize storage handler
        
        Args:
            base_path: Base directory for file storage
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def save_file(self, file, job_id, filename):
        """
        Save uploaded file to storage
        
        Args:
            file: FileStorage object from Flask
            job_id: Unique job identifier
            filename: Original filename
            
        Returns:
            Path to saved file
        """
        # Create job directory
        job_dir = self.base_path / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        
        # Save file
        filepath = job_dir / filename
        file.save(str(filepath))
        
        return str(filepath)
    
    def get_file_path(self, job_id, filename):
        """
        Get path to a specific file
        
        Args:
            job_id: Job identifier
            filename: File name
            
        Returns:
            Path to file
        """
        return str(self.base_path / job_id / filename)
    
    def save_results(self, job_id, results, filename='results.json'):
        """
        Save processing results to file
        
        Args:
            job_id: Job identifier
            results: Results dictionary
            filename: Output filename
            
        Returns:
            Path to results file
        """
        import json
        
        job_dir = self.base_path / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        
        results_path = job_dir / filename
        
        with open(results_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        return str(results_path)
    
    def load_results(self, job_id, filename='results.json'):
        """
        Load results from file
        
        Args:
            job_id: Job identifier
            filename: Results filename
            
        Returns:
            Results dictionary
        """
        import json
        
        results_path = self.base_path / job_id / filename
        
        if not results_path.exists():
            return None
        
        with open(results_path, 'r') as f:
            return json.load(f)
    
    def delete_job(self, job_id):
        """
        Delete all files for a job
        
        Args:
            job_id: Job identifier
        """
        job_dir = self.base_path / job_id
        
        if job_dir.exists():
            shutil.rmtree(job_dir)
    
    def cleanup_old_jobs(self, days=7):
        """
        Remove jobs older than specified days
        
        Args:
            days: Number of days to keep
        """
        current_time = datetime.now().timestamp()
        cutoff_time = current_time - (days * 24 * 60 * 60)
        
        for job_dir in self.base_path.iterdir():
            if job_dir.is_dir():
                # Check directory modification time
                dir_mtime = job_dir.stat().st_mtime
                
                if dir_mtime < cutoff_time:
                    shutil.rmtree(job_dir)
    
    def get_storage_info(self):
        """
        Get storage information
        
        Returns:
            Dictionary with storage stats
        """
        total_size = 0
        file_count = 0
        job_count = 0
        
        for job_dir in self.base_path.iterdir():
            if job_dir.is_dir():
                job_count += 1
                for file_path in job_dir.rglob('*'):
                    if file_path.is_file():
                        total_size += file_path.stat().st_size
                        file_count += 1
        
        return {
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'file_count': file_count,
            'job_count': job_count,
            'base_path': str(self.base_path)
        }


class CloudStorageHandler:
    """
    Handler for cloud storage operations (AWS S3, GCP, Azure)
    This is a template - implement specific cloud provider methods as needed
    """
    
    def __init__(self, provider='s3', **config):
        """
        Initialize cloud storage handler
        
        Args:
            provider: Cloud provider ('s3', 'gcs', 'azure')
            config: Provider-specific configuration
        """
        self.provider = provider
        self.config = config
        self.client = self._init_client()
    
    def _init_client(self):
        """Initialize cloud storage client based on provider"""
        if self.provider == 's3':
            import boto3
            return boto3.client(
                's3',
                aws_access_key_id=self.config.get('access_key'),
                aws_secret_access_key=self.config.get('secret_key'),
                region_name=self.config.get('region', 'us-east-1')
            )
        elif self.provider == 'gcs':
            from google.cloud import storage
            return storage.Client(project=self.config.get('project_id'))
        elif self.provider == 'azure':
            from azure.storage.blob import BlobServiceClient
            return BlobServiceClient.from_connection_string(
                self.config.get('connection_string')
            )
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")
    
    def upload_file(self, local_path, remote_path):
        """Upload file to cloud storage"""
        if self.provider == 's3':
            bucket = self.config.get('bucket')
            self.client.upload_file(local_path, bucket, remote_path)
        elif self.provider == 'gcs':
            bucket = self.client.bucket(self.config.get('bucket'))
            blob = bucket.blob(remote_path)
            blob.upload_from_filename(local_path)
        elif self.provider == 'azure':
            container = self.config.get('container')
            blob_client = self.client.get_blob_client(
                container=container,
                blob=remote_path
            )
            with open(local_path, 'rb') as data:
                blob_client.upload_blob(data, overwrite=True)
    
    def download_file(self, remote_path, local_path):
        """Download file from cloud storage"""
        if self.provider == 's3':
            bucket = self.config.get('bucket')
            self.client.download_file(bucket, remote_path, local_path)
        elif self.provider == 'gcs':
            bucket = self.client.bucket(self.config.get('bucket'))
            blob = bucket.blob(remote_path)
            blob.download_to_filename(local_path)
        elif self.provider == 'azure':
            container = self.config.get('container')
            blob_client = self.client.get_blob_client(
                container=container,
                blob=remote_path
            )
            with open(local_path, 'wb') as data:
                data.write(blob_client.download_blob().readall())