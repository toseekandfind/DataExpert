from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, count, when, sum, avg
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_player_seasons_schema():
    """
    Creates and returns the schema for player_seasons table
    
    Returns:
        StructType: Schema for the player_seasons data
    """
    return StructType([
        StructField("player_name", StringType(), False),  # NOT NULL
        StructField("age", IntegerType(), True),
        StructField("height", StringType(), True),
        StructField("weight", IntegerType(), True),
        StructField("college", StringType(), True),
        StructField("country", StringType(), True),
        StructField("draft_year", StringType(), True),
        StructField("draft_round", StringType(), True),
        StructField("draft_number", StringType(), True),
        StructField("gp", FloatType(), True),
        StructField("pts", FloatType(), True),
        StructField("reb", FloatType(), True),
        StructField("ast", FloatType(), True),
        StructField("netrtg", FloatType(), True),
        StructField("oreb_pct", FloatType(), True),
        StructField("dreb_pct", FloatType(), True),
        StructField("usg_pct", FloatType(), True),
        StructField("ts_pct", FloatType(), True),
        StructField("ast_pct", FloatType(), True),
        StructField("season", IntegerType(), False)  # NOT NULL
    ])

def validate_data_quality(df):
    """
    Performs data quality checks and logs metrics
    
    Args:
        df: DataFrame to validate
        
    Returns:
        bool: True if data quality checks pass, False otherwise
    """
    total_rows = df.count()
    
    # Calculate quality metrics
    quality_metrics = df.agg(
        count("*").alias("total_rows"),
        sum(when(col("player_name").isNull(), 1).otherwise(0)).alias("null_names"),
        sum(when(col("season").isNull(), 1).otherwise(0)).alias("null_seasons"),
        avg("pts").alias("avg_points"),
        avg("age").alias("avg_age")
    ).collect()[0]
    
    # Log metrics
    logger.info(f"Data Quality Metrics:")
    logger.info(f"Total Rows: {quality_metrics['total_rows']}")
    logger.info(f"Null Names: {quality_metrics['null_names']}")
    logger.info(f"Null Seasons: {quality_metrics['null_seasons']}")
    logger.info(f"Average Points: {quality_metrics['avg_points']:.2f}")
    logger.info(f"Average Age: {quality_metrics['avg_age']:.2f}")
    
    # Define quality thresholds
    if quality_metrics['null_names'] > 0 or quality_metrics['null_seasons'] > 0:
        logger.error("Data quality check failed: Found null values in required fields")
        return False
    
    return True

def process_player_seasons(spark: SparkSession, input_path: str, output_path: str):
    """
    Process player_seasons data using PySpark with optimizations
    
    Args:
        spark: SparkSession object
        input_path: Path to input data
        output_path: Path where processed data should be saved
        
    Returns:
        bool: True if processing succeeds, False otherwise
    """
    try:
        logger.info(f"Starting player seasons processing job")
        
        # Configure for better performance
        spark.conf.set("spark.sql.shuffle.partitions", "200")  # Adjust based on data size
        spark.conf.set("spark.sql.adaptive.enabled", "true")   # Enable adaptive query execution
        
        # Define schema
        schema = create_player_seasons_schema()
        
        # Read data with schema
        logger.info(f"Reading data from {input_path}")
        df = spark.read.schema(schema) \
            .format("csv") \
            .option("header", "true") \
            .option("mode", "DROPMALFORMED") \
            .load(input_path)
        
        # Cache the DataFrame if we're going to use it multiple times
        df.cache()
        
        # Validate data quality
        if not validate_data_quality(df):
            logger.error("Data quality validation failed")
            return False
        
        # Clean data - remove nulls in required fields
        df_clean = df.dropna(subset=["player_name", "season"])
        
        # Repartition before writing if needed
        num_partitions = max(1, df_clean.count() // 1000000)  # 1 partition per million rows
        df_final = df_clean.repartition(num_partitions)
        
        # Write the processed data
        logger.info(f"Writing processed data to {output_path}")
        df_final.write \
            .mode("overwrite") \
            .format("parquet") \
            .option("compression", "snappy") \
            .save(output_path)
        
        # Unpersist cached data
        df.unpersist()
        
        logger.info("Player seasons processing completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error processing player seasons: {str(e)}")
        raise
    
def main():
    """Main function to run the job"""
    spark = SparkSession.builder \
        .appName("PlayerSeasonsJob") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "2g") \
        .getOrCreate()
    
    try:
        # TODO: Replace with actual input/output paths
        input_path = "path/to/input/data"
        output_path = "path/to/output/data"
        
        success = process_player_seasons(spark, input_path, output_path)
        if not success:
            logger.error("Job failed due to data quality issues")
            spark.stop()
            exit(1)
            
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        spark.stop()
        raise
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 