"""
Machine Learning Jobs Module
Implements various ML algorithms using Spark MLlib
"""

from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.fpm import FPGrowth
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.sql.functions import col, array, when
from pyspark.sql.types import NumericType
import random

class MLJobs:
    """Class for running machine learning jobs"""
    
    def __init__(self, spark):
        """Initialize with Spark session"""
        self.spark = spark
    
    def _prepare_features(self, df, label_col=None):
        """
        Prepare feature vectors from DataFrame
        
        Args:
            df: Spark DataFrame
            label_col: Name of label column (optional)
            
        Returns:
            DataFrame with features vector
        """
        # Get numeric columns
        numeric_cols = [field.name for field in df.schema.fields 
                       if isinstance(field.dataType, NumericType)]
        
        # Remove label column from features if specified
        if label_col and label_col in numeric_cols:
            numeric_cols.remove(label_col)
        
        if not numeric_cols:
            raise ValueError("No numeric columns found for feature preparation")
        
        # Take first 10 columns to avoid too many features
        feature_cols = numeric_cols[:10]
        
        # Handle null values
        df = df.na.fill(0, subset=feature_cols)
        
        # Create vector assembler
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df = assembler.transform(df)
        
        return df, feature_cols
    
    def linear_regression(self, df):
        """
        Train a Linear Regression model
        
        Args:
            df: Spark DataFrame
            
        Returns:
            Dictionary with model results
        """
        # Get numeric columns
        numeric_cols = [field.name for field in df.schema.fields 
                       if isinstance(field.dataType, NumericType)]
        
        if len(numeric_cols) < 2:
            return {'error': 'Need at least 2 numeric columns for regression'}
        
        # Use first column as label, rest as features
        label_col = numeric_cols[0]
        
        # Prepare features
        df, feature_cols = self._prepare_features(df, label_col)
        df = df.withColumnRenamed(label_col, "label")
        
        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        # Train model
        lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
        model = lr.fit(train_df)
        
        # Make predictions
        predictions = model.transform(test_df)
        
        # Evaluate
        evaluator = RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="rmse"
        )
        rmse = evaluator.evaluate(predictions)
        
        evaluator.setMetricName("r2")
        r2 = evaluator.evaluate(predictions)
        
        return {
            'Model': 'Linear Regression',
            'Features Used': feature_cols,
            'Label Column': label_col,
            'Training Samples': train_df.count(),
            'Test Samples': test_df.count(),
            'RMSE': round(rmse, 4),
            'R² Score': round(r2, 4),
            'Coefficients': [round(float(c), 4) for c in model.coefficients.toArray()[:5]],
            'Intercept': round(float(model.intercept), 4)
        }
    
    def kmeans_clustering(self, df, k=3):
        """
        Perform K-Means clustering
        
        Args:
            df: Spark DataFrame
            k: Number of clusters
            
        Returns:
            Dictionary with clustering results
        """
        # Prepare features
        df, feature_cols = self._prepare_features(df)
        
        # Train K-Means
        kmeans = KMeans(k=k, seed=42, maxIter=20)
        model = kmeans.fit(df)
        
        # Make predictions
        predictions = model.transform(df)
        
        # Get cluster centers
        centers = model.clusterCenters()
        
        # Count samples per cluster
        cluster_counts = predictions.groupBy("prediction").count().collect()
        cluster_distribution = {f"Cluster {row['prediction']}": row['count'] 
                              for row in cluster_counts}
        
        return {
            'Model': 'K-Means Clustering',
            'Number of Clusters': k,
            'Features Used': feature_cols,
            'Total Samples': df.count(),
            'Within Set Sum of Squared Errors': round(model.summary.trainingCost, 4),
            'Cluster Distribution': cluster_distribution,
            'Cluster Centers (first 3 dims)': [[round(float(c), 4) for c in center[:3]] 
                                               for center in centers]
        }
    
    def decision_tree(self, df):
        """
        Train a Decision Tree classifier
        
        Args:
            df: Spark DataFrame
            
        Returns:
            Dictionary with classification results
        """
        # Get numeric columns
        numeric_cols = [field.name for field in df.schema.fields 
                       if isinstance(field.dataType, NumericType)]
        
        if len(numeric_cols) < 2:
            return {'error': 'Need at least 2 numeric columns for classification'}
        
        # Use first column as label (create binary classification)
        label_col = numeric_cols[0]
        
        # Prepare features
        df, feature_cols = self._prepare_features(df, label_col)
        
        # Create binary labels based on median
        median_val = df.approxQuantile(label_col, [0.5], 0.01)[0]
        df = df.withColumn("label", when(col(label_col) > median_val, 1.0).otherwise(0.0))
        
        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        # Train Decision Tree
        dt = DecisionTreeClassifier(maxDepth=5, maxBins=32)
        model = dt.fit(train_df)
        
        # Make predictions
        predictions = model.transform(test_df)
        
        # Evaluate
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)
        
        evaluator.setMetricName("f1")
        f1 = evaluator.evaluate(predictions)
        
        return {
            'Model': 'Decision Tree Classifier',
            'Features Used': feature_cols,
            'Original Label Column': label_col,
            'Classification Threshold': round(median_val, 4),
            'Training Samples': train_df.count(),
            'Test Samples': test_df.count(),
            'Accuracy': round(accuracy, 4),
            'F1 Score': round(f1, 4),
            'Max Depth': model.depth,
            'Number of Nodes': model.numNodes
        }
    
    def fp_growth(self, df, min_support=0.3, min_confidence=0.6):
        """
        Find frequent itemsets using FP-Growth
        
        Args:
            df: Spark DataFrame
            min_support: Minimum support threshold
            min_confidence: Minimum confidence threshold
            
        Returns:
            Dictionary with frequent patterns
        """
        # For FP-Growth, we need transaction data
        # Convert DataFrame columns to items
        
        # Get first 5 columns
        cols_to_use = df.columns[:5]
        
        # Create transactions by combining column values
        # Each row becomes a transaction with items as "column_value" format
        transactions = df.select(cols_to_use).rdd.map(
            lambda row: [f"{col}_{val}" for col, val in zip(cols_to_use, row) if val is not None]
        ).toDF(["items"])
        
        # Remove empty transactions
        transactions = transactions.filter(col("items").isNotNull())
        
        # Train FP-Growth
        fpGrowth = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence)
        model = fpGrowth.fit(transactions)
        
        # Get frequent itemsets
        freq_itemsets = model.freqItemsets.orderBy(col("freq").desc()).limit(10).collect()
        
        # Get association rules
        rules = model.associationRules.orderBy(col("confidence").desc()).limit(10).collect()
        
        frequent_patterns = {}
        for i, row in enumerate(freq_itemsets):
            items = ', '.join(row['items'][:3])  # Show first 3 items
            frequent_patterns[f"Pattern {i+1}"] = {
                'Items': items,
                'Frequency': row['freq']
            }
        
        association_rules = {}
        for i, row in enumerate(rules):
            antecedent = ', '.join(row['antecedent'][:2])
            consequent = ', '.join(row['consequent'][:2])
            association_rules[f"Rule {i+1}"] = {
                'If': antecedent,
                'Then': consequent,
                'Confidence': round(float(row['confidence']), 4)
            }
        
        return {
            'Model': 'FP-Growth (Frequent Pattern Mining)',
            'Min Support': min_support,
            'Min Confidence': min_confidence,
            'Total Transactions': transactions.count(),
            'Number of Frequent Itemsets': model.freqItemsets.count(),
            'Top Frequent Patterns': frequent_patterns,
            'Top Association Rules': association_rules
        }