"""
Performance Tracker Module
Tracks and analyzes performance metrics for Spark jobs
"""

import time
from datetime import datetime
import psutil
import json

class PerformanceTracker:
    """Tracks performance metrics during job execution"""
    
    def __init__(self):
        """Initialize performance tracker"""
        self.metrics = []
        self.start_time = None
        self.baseline_time = {}
    
    def start(self, job_name):
        """
        Start tracking a job
        
        Args:
            job_name: Name of the job
            
        Returns:
            Metric ID
        """
        metric_id = len(self.metrics)
        
        metric = {
            'id': metric_id,
            'job_name': job_name,
            'start_time': time.time(),
            'start_datetime': datetime.now().isoformat(),
            'end_time': None,
            'execution_time': None,
            'cpu_percent_start': psutil.cpu_percent(interval=1),
            'memory_percent_start': psutil.virtual_memory().percent,
            'cpu_percent_end': None,
            'memory_percent_end': None
        }
        
        self.metrics.append(metric)
        return metric_id
    
    def stop(self, metric_id):
        """
        Stop tracking a job
        
        Args:
            metric_id: ID of the metric to stop
        """
        if metric_id < len(self.metrics):
            metric = self.metrics[metric_id]
            metric['end_time'] = time.time()
            metric['execution_time'] = metric['end_time'] - metric['start_time']
            metric['cpu_percent_end'] = psutil.cpu_percent(interval=1)
            metric['memory_percent_end'] = psutil.virtual_memory().percent
    
    def calculate_speedup(self, job_name, workers, execution_time):
        """
        Calculate speedup and efficiency
        
        Args:
            job_name: Name of the job
            workers: Number of workers used
            execution_time: Execution time in seconds
            
        Returns:
            Dictionary with speedup metrics
        """
        # Store baseline (single worker) time
        if workers == 1:
            self.baseline_time[job_name] = execution_time
            return {
                'speedup': 1.0,
                'efficiency': 100.0,
                'baseline_time': execution_time
            }
        
        # Calculate speedup relative to baseline
        baseline = self.baseline_time.get(job_name, execution_time)
        speedup = baseline / execution_time if execution_time > 0 else 1.0
        efficiency = (speedup / workers) * 100
        
        return {
            'speedup': speedup,
            'efficiency': efficiency,
            'baseline_time': baseline
        }
    
    def get_metrics(self):
        """
        Get all collected metrics
        
        Returns:
            List of metrics dictionaries
        """
        return self.metrics
    
    def get_summary(self):
        """
        Get summary statistics of all metrics
        
        Returns:
            Dictionary with summary statistics
        """
        if not self.metrics:
            return {'message': 'No metrics collected'}
        
        completed_metrics = [m for m in self.metrics if m['execution_time'] is not None]
        
        if not completed_metrics:
            return {'message': 'No completed jobs'}
        
        total_time = sum(m['execution_time'] for m in completed_metrics)
        avg_time = total_time / len(completed_metrics)
        min_time = min(m['execution_time'] for m in completed_metrics)
        max_time = max(m['execution_time'] for m in completed_metrics)
        
        return {
            'total_jobs': len(completed_metrics),
            'total_time': round(total_time, 2),
            'average_time': round(avg_time, 2),
            'min_time': round(min_time, 2),
            'max_time': round(max_time, 2)
        }
    
    def generate_report(self):
        """
        Generate a detailed performance report
        
        Returns:
            Formatted string report
        """
        report = []
        report.append("=" * 80)
        report.append("PERFORMANCE REPORT")
        report.append("=" * 80)
        report.append("")
        
        summary = self.get_summary()
        report.append("Summary:")
        for key, value in summary.items():
            report.append(f"  {key}: {value}")
        report.append("")
        
        report.append("Detailed Metrics:")
        report.append("-" * 80)
        
        for metric in self.metrics:
            if metric['execution_time'] is not None:
                report.append(f"\nJob: {metric['job_name']}")
                report.append(f"  Execution Time: {metric['execution_time']:.2f}s")
                report.append(f"  CPU Usage: {metric['cpu_percent_start']:.1f}% -> "
                            f"{metric['cpu_percent_end']:.1f}%")
                report.append(f"  Memory Usage: {metric['memory_percent_start']:.1f}% -> "
                            f"{metric['memory_percent_end']:.1f}%")
        
        return "\n".join(report)
    
    def save_to_file(self, filepath):
        """
        Save metrics to JSON file
        
        Args:
            filepath: Path to save file
        """
        with open(filepath, 'w') as f:
            json.dump({
                'metrics': self.metrics,
                'summary': self.get_summary()
            }, f, indent=2)
    
    def reset(self):
        """Reset all metrics"""
        self.metrics = []
        self.baseline_time = {}


class ScalabilityAnalyzer:
    """Analyzes scalability based on performance metrics"""
    
    @staticmethod
    def analyze_scalability(performance_data):
        """
        Analyze scalability from performance data
        
        Args:
            performance_data: List of dicts with workers, execution_time, speedup, efficiency
            
        Returns:
            Dictionary with scalability analysis
        """
        if not performance_data or len(performance_data) < 2:
            return {'message': 'Insufficient data for scalability analysis'}
        
        # Sort by workers
        data = sorted(performance_data, key=lambda x: x['workers'])
        
        # Calculate metrics
        max_speedup = max(d['speedup'] for d in data)
        avg_efficiency = sum(d['efficiency'] for d in data) / len(data)
        
        # Determine scalability type
        if avg_efficiency >= 85:
            scalability_type = "Excellent (Near-linear scalability)"
            recommendation = "System scales very well. Consider adding more workers for larger datasets."
        elif avg_efficiency >= 70:
            scalability_type = "Good (Effective parallelization)"
            recommendation = "Good scalability. Current configuration is efficient."
        elif avg_efficiency >= 50:
            scalability_type = "Fair (Moderate scalability)"
            recommendation = "Consider optimizing data partitioning or algorithm implementation."
        else:
            scalability_type = "Poor (Limited benefits)"
            recommendation = "Parallelization overhead may be too high. Review algorithm and data size."
        
        # Calculate Amdahl's law estimation
        # S(n) = 1 / ((1-p) + p/n) where p is parallel portion
        # Estimate p from speedup data
        if len(data) >= 2:
            s_n = data[-1]['speedup']
            n = data[-1]['workers']
            # Approximate parallel portion
            p = (s_n * (n - 1)) / (n * s_n - 1) if (n * s_n - 1) > 0 else 0.8
            p = max(0, min(1, p))  # Clamp between 0 and 1
            
            # Theoretical maximum speedup
            theoretical_max = 1 / (1 - p) if p < 1 else float('inf')
        else:
            p = 0.8
            theoretical_max = 5.0
        
        return {
            'scalability_type': scalability_type,
            'max_speedup_achieved': round(max_speedup, 2),
            'average_efficiency': round(avg_efficiency, 2),
            'estimated_parallel_portion': round(p * 100, 2),
            'theoretical_max_speedup': round(theoretical_max, 2) if theoretical_max != float('inf') else 'Unlimited',
            'recommendation': recommendation,
            'detailed_metrics': data
        }