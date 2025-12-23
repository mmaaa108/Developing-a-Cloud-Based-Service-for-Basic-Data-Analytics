"""
Statistics Module
Computes various descriptive statistics on Spark DataFrames
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, stddev, min, max, countDistinct
from pyspark.sql.types import NumericType

class Statistics:
    """Class for computing descriptive statistics"""
    
    def __init__(self, spark):
        """Initialize with Spark session"""
        self.spark = spark
    
    def basic_info(self, df):
        """
        Compute basic information about the dataset
        
        Args:
            df: Spark DataFrame
            
        Returns:
            Dictionary with basic information
        """
        num_rows = df.count()
        num_columns = len(df.columns)
        
        # Get data types
        data_types = {}
        numeric_cols = []
        categorical_cols = []
        
        for field in df.schema.fields:
            col_name = field.name
            col_type = str(field.dataType)
            data_types[col_name] = col_type
            
            if isinstance(field.dataType, NumericType):
                numeric_cols.append(col_name)
            else:
                categorical_cols.append(col_name)
        
        # Calculate size in memory (approximate)
        sample = df.limit(1000).toPandas()
        size_mb = (sample.memory_usage(deep=True).sum() / 1024 / 1024) * (num_rows / 1000)
        
        return {
            'Number of Rows': num_rows,
            'Number of Columns': num_columns,
            'Numeric Columns': len(numeric_cols),
            'Categorical Columns': len(categorical_cols),
            'Estimated Size (MB)': round(size_mb, 2),
            'Column Names': df.columns,
            'Data Types': data_types
        }
    
    def summary_statistics(self, df):
        """
        Compute summary statistics for numeric columns
        
        Args:
            df: Spark DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        # Get numeric columns
        numeric_cols = [field.name for field in df.schema.fields 
                       if isinstance(field.dataType, NumericType)]
        
        if not numeric_cols:
            return {'message': 'No numeric columns found in dataset'}
        
        # Select first few numeric columns to avoid too much computation
        cols_to_analyze = numeric_cols[:10]
        
        stats = {}
        
        for col_name in cols_to_analyze:
            # Compute statistics for each column
            col_stats = df.select(
                min(col(col_name)).alias('min'),
                max(col(col_name)).alias('max'),
                mean(col(col_name)).alias('mean'),
                stddev(col(col_name)).alias('stddev')
            ).first()
            
            stats[col_name] = {
                'Min': float(col_stats['min']) if col_stats['min'] is not None else None,
                'Max': float(col_stats['max']) if col_stats['max'] is not None else None,
                'Mean': float(col_stats['mean']) if col_stats['mean'] is not None else None,
                'Std Dev': float(col_stats['stddev']) if col_stats['stddev'] is not None else None
            }
        
        return stats
    
    def missing_values(self, df):
        """
        Analyze missing values in the dataset
        
        Args:
            df: Spark DataFrame
            
        Returns:
            Dictionary with missing value statistics
        """
        total_rows = df.count()
        
        missing_stats = {}
        
        for col_name in df.columns:
            # Count nulls
            null_count = df.filter(col(col_name).isNull()).count()
            null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
            
            missing_stats[col_name] = {
                'Missing Count': null_count,
                'Missing Percentage': round(null_percentage, 2)
            }
        
        # Calculate overall statistics
        total_missing = sum(s['Missing Count'] for s in missing_stats.values())
        total_cells = total_rows * len(df.columns)
        overall_percentage = (total_missing / total_cells * 100) if total_cells > 0 else 0
        
        return {
            'Column Statistics': missing_stats,
            'Total Missing Values': total_missing,
            'Overall Missing Percentage': round(overall_percentage, 2)
        }
    
    def unique_values(self, df):
        """
        Count unique values in each column
        
        Args:
            df: Spark DataFrame
            
        Returns:
            Dictionary with unique value counts
        """
        unique_stats = {}
        total_rows = df.count()
        
        for col_name in df.columns:
            # Count distinct values
            distinct_count = df.select(col_name).distinct().count()
            uniqueness_ratio = (distinct_count / total_rows * 100) if total_rows > 0 else 0
            
            unique_stats[col_name] = {
                'Unique Count': distinct_count,
                'Uniqueness Ratio (%)': round(uniqueness_ratio, 2)
            }
        
        return unique_stats
    
    def correlation_matrix(self, df):
        """
        Compute correlation matrix for numeric columns
        
        Args:
            df: Spark DataFrame
            
        Returns:
            Dictionary with correlation values
        """
        # Get numeric columns
        numeric_cols = [field.name for field in df.schema.fields 
                       if isinstance(field.dataType, NumericType)]
        
        if len(numeric_cols) < 2:
            return {'message': 'Need at least 2 numeric columns for correlation'}
        
        # Limit to first 5 columns to avoid excessive computation
        cols_to_analyze = numeric_cols[:5]
        
        correlations = {}
        
        for i, col1 in enumerate(cols_to_analyze):
            for col2 in cols_to_analyze[i+1:]:
                corr = df.stat.corr(col1, col2)
                correlations[f'{col1} vs {col2}'] = round(corr, 4)
        
        return correlations
    
    def value_counts(self, df, column, top_n=10):
        """
        Get value counts for a specific column
        
        Args:
            df: Spark DataFrame
            column: Column name
            top_n: Number of top values to return
            
        Returns:
            Dictionary with value counts
        """
        value_counts = (df.groupBy(column)
                       .count()
                       .orderBy(col('count').desc())
                       .limit(top_n)
                       .collect())
        
        result = {}
        for row in value_counts:
            result[str(row[column])] = row['count']
        
        return result