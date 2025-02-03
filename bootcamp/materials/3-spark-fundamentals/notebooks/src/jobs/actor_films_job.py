from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

"""
Approaches to Working with Spark:

1. DataFrame API with StructType (Our Chosen Approach):
   - Provides schema validation at read time
   - Offers type safety without the verbosity of Datasets
   - Example:
   ```python
   schema = StructType([
       StructField("actor", StringType(), True),
       StructField("actor_id", StringType(), True)
   ])
   df = spark.read.schema(schema).csv("path")
   df.filter(F.col("actor_id").isNotNull())
   ```

2. SparkSQL with Temp Views:
   - More familiar for SQL developers
   - Less type safety but more flexible for complex queries
   - Example:
   ```python
   df.createOrReplaceTempView("actor_films")
   result = spark.sql('''
       SELECT actor_id, actor,
              COUNT(DISTINCT film_id) as num_films
       FROM actor_films
       GROUP BY actor_id, actor
   ''')
   ```

3. Raw DataFrame without Schema:
   - Relies on schema inference
   - Less type safety, potential runtime errors
   - Example:
   ```python
   df = spark.read.csv("path")
   df.join(other_df, "actor_id")  # No compile-time type checking
   ```

Why We Chose StructType Approach:
1. Schema Validation: Catches data issues at read time rather than processing time
2. Type Safety: Provides compile-time checking for column names and types
3. Performance: Avoids the overhead of schema inference
4. IDE Support: Better code completion and error detection
5. Maintainability: Schema changes are explicit and version controlled

Trade-offs:
- More verbose initial setup compared to schema inference
- Less flexible than raw SQL for complex ad-hoc queries
- Requires more Python code compared to SQL approach
"""

def create_actor_films_schema():
    """
    Creates and returns the schema for actor_films table
    
    Returns:
        StructType: Schema for the actor_films data with NOT NULL constraints on key fields
    """
    return StructType([
        StructField("actor", StringType(), False),  # NOT NULL
        StructField("actor_id", StringType(), False),  # NOT NULL
        StructField("film", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("votes", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("film_id", StringType(), False),  # NOT NULL
        StructField("last_updated", TimestampType(), True)  # For SCD tracking
    ])

def create_actor_history_scd_schema():
    """
    Creates and returns the schema for actor_history_scd table (Type 2 SCD)
    
    Returns:
        StructType: Schema for the actor history SCD table
    """
    return StructType([
        StructField("actor_id", StringType(), False),  # NOT NULL
        StructField("actor", StringType(), False),  # NOT NULL
        StructField("total_films", IntegerType(), True),
        StructField("avg_rating", FloatType(), True),
        StructField("total_votes", IntegerType(), True),
        StructField("valid_from", TimestampType(), False),  # NOT NULL
        StructField("valid_to", TimestampType(), True),
        StructField("is_current", StringType(), False)  # NOT NULL
    ])

def validate_data_quality(df, table_name="actor_films"):
    """
    Performs comprehensive data quality checks and logs metrics
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table being validated
        
    Returns:
        bool: True if data quality checks pass, False otherwise
    """
    try:
        # Calculate basic quality metrics
        row_count = df.count()
        if row_count == 0:
            logger.error(f"No data found in {table_name}")
            return False

        # Calculate detailed metrics
        metrics = df.agg(
            F.count("*").alias("total_rows"),
            F.sum(F.when(F.col("actor").isNull(), 1).otherwise(0)).alias("null_actors"),
            F.sum(F.when(F.col("actor_id").isNull(), 1).otherwise(0)).alias("null_actor_ids"),
            F.sum(F.when(F.col("film_id").isNull(), 1).otherwise(0)).alias("null_film_ids"),
            F.avg("rating").alias("avg_rating"),
            F.stddev("rating").alias("rating_stddev"),
            F.avg("votes").alias("avg_votes"),
            F.min("year").alias("min_year"),
            F.max("year").alias("max_year")
        ).collect()[0]

        # Log comprehensive metrics
        logger.info(f"Data Quality Metrics for {table_name}:")
        logger.info(f"Total Rows: {metrics['total_rows']}")
        logger.info(f"Null Values:")
        logger.info(f"  - Actors: {metrics['null_actors']}")
        logger.info(f"  - Actor IDs: {metrics['null_actor_ids']}")
        logger.info(f"  - Film IDs: {metrics['null_film_ids']}")
        logger.info(f"Rating Statistics:")
        logger.info(f"  - Average: {metrics['avg_rating']:.2f}")
        logger.info(f"  - Standard Deviation: {metrics['rating_stddev']:.2f}")
        logger.info(f"Votes Average: {metrics['avg_votes']:.0f}")
        logger.info(f"Year Range: {metrics['min_year']} - {metrics['max_year']}")

        # Validate data quality rules
        quality_checks = [
            (metrics['null_actors'] == 0, "Found null actors"),
            (metrics['null_actor_ids'] == 0, "Found null actor IDs"),
            (metrics['null_film_ids'] == 0, "Found null film IDs"),
            (metrics['min_year'] >= 1900, "Invalid year (before 1900)"),
            (metrics['max_year'] <= datetime.now().year, "Invalid year (future)"),
            (metrics['avg_rating'] >= 0 and metrics['avg_rating'] <= 10, "Rating out of range (0-10)")
        ]

        # Check all quality rules
        failed_checks = [msg for check, msg in quality_checks if not check]
        if failed_checks:
            for msg in failed_checks:
                logger.error(f"Data quality check failed: {msg}")
            return False

        # Additional checks for duplicates
        duplicates = df.groupBy("actor_id", "film_id").count().filter("count > 1")
        if duplicates.count() > 0:
            logger.error(f"Found duplicate actor-film combinations")
            return False

        logger.info(f"All data quality checks passed for {table_name}")
        return True

    except Exception as e:
        logger.error(f"Error during data quality validation: {str(e)}")
        return False

def analyze_actor_statistics(df):
    """
    Analyze actor statistics using DataFrame API with performance optimizations
    
    Args:
        df: DataFrame containing actor films data
        
    Returns:
        DataFrame: Aggregated actor statistics with career metrics
    """
    # Cache the DataFrame for multiple operations
    df.cache()
    
    try:
        # Calculate base statistics
        stats = df.groupBy("actor_id", "actor") \
            .agg(
                F.countDistinct("film_id").alias("num_films"),
                F.avg("rating").alias("avg_rating"),
                F.sum("votes").alias("total_votes"),
                F.min("year").alias("first_film_year"),
                F.max("year").alias("last_film_year"),
                F.collect_set("film").alias("films")
            )

        # Add derived metrics
        stats = stats \
            .withColumn("career_span_years",
                       F.col("last_film_year") - F.col("first_film_year")) \
            .withColumn("films_per_year",
                       F.col("num_films") / F.greatest(F.lit(1), F.col("career_span_years"))) \
            .withColumn("weighted_rating",
                       F.expr("avg_rating * log10(greatest(total_votes, 1))"))

        # Filter and sort
        return stats \
            .where(F.col("num_films") > 1) \
            .orderBy(F.desc("weighted_rating"))

    finally:
        # Ensure cache is cleared
        df.unpersist()

def process_actor_films(spark: SparkSession, input_path: str, output_path: str, history_table_path: str = None):
    """
    Process actor_films data with SCD Type 2 implementation
    
    Args:
        spark: SparkSession object
        input_path: Path to input data
        output_path: Path where processed data should be saved
        history_table_path: Optional path to existing history table
        
    Returns:
        bool: True if processing succeeds, False otherwise
    """
    try:
        logger.info(f"Starting actor films processing job")
        
        # Configure for better performance
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.shuffle.partitions", "200")
        
        # Read and validate input data
        logger.info(f"Reading data from {input_path}")
        current_timestamp = F.current_timestamp()
        
        df = spark.read.schema(create_actor_films_schema()) \
            .format("csv") \
            .option("header", "true") \
            .option("mode", "DROPMALFORMED") \
            .load(input_path) \
            .withColumn("last_updated", current_timestamp)
        
        if not validate_data_quality(df):
            return False
        
        # Clean data
        df_clean = df.dropna(subset=["actor_id", "film_id"])
        
        # Process current statistics
        current_stats = analyze_actor_statistics(df_clean)
        
        # Implement SCD Type 2 if history table path provided
        if history_table_path:
            try:
                # Read existing history
                history_df = spark.read \
                    .schema(create_actor_history_scd_schema()) \
                    .format("parquet") \
                    .load(history_table_path)
                
                # Identify changes
                current_stats = current_stats.select(
                    "actor_id", "actor", 
                    F.col("num_films").alias("total_films"),
                    "avg_rating", "total_votes"
                )
                
                # Find changed records
                changed_records = current_stats \
                    .join(
                        history_df.filter(F.col("is_current") == "Y"),
                        ["actor_id"],
                        "full_outer"
                    ) \
                    .where(
                        (F.col("actor") != F.col("actor_prev")) |
                        (F.col("total_films") != F.col("total_films_prev")) |
                        (F.abs(F.col("avg_rating") - F.col("avg_rating_prev")) > 0.1) |
                        (F.col("total_votes") != F.col("total_votes_prev"))
                    )
                
                # Update history
                # Close current records
                updates = history_df \
                    .filter(F.col("is_current") == "Y") \
                    .join(changed_records.select("actor_id"), ["actor_id"]) \
                    .withColumn("valid_to", current_timestamp) \
                    .withColumn("is_current", F.lit("N"))
                
                # Insert new records
                inserts = current_stats \
                    .join(changed_records.select("actor_id"), ["actor_id"]) \
                    .select(
                        "actor_id", "actor", "total_films", "avg_rating", "total_votes",
                        current_timestamp.alias("valid_from"),
                        F.lit(None).cast(TimestampType()).alias("valid_to"),
                        F.lit("Y").alias("is_current")
                    )
                
                # Combine all records
                history_df = history_df \
                    .filter(~F.col("actor_id").isin(changed_records.select("actor_id"))) \
                    .unionAll(updates) \
                    .unionAll(inserts)
                
                # Write updated history
                history_df.write \
                    .mode("overwrite") \
                    .format("parquet") \
                    .option("compression", "snappy") \
                    .save(history_table_path)
                
            except Exception as e:
                logger.error(f"Error processing SCD updates: {str(e)}")
                raise
        
        # Optimize partitioning for writing
        partition_count = max(1, df_clean.count() // 1000000)
        df_clean = df_clean.repartition(partition_count)
        current_stats = current_stats.coalesce(max(1, partition_count // 10))
        
        # Write processed data
        logger.info(f"Writing processed data to {output_path}")
        df_clean.write \
            .mode("overwrite") \
            .format("parquet") \
            .option("compression", "snappy") \
            .save(f"{output_path}/raw")
            
        current_stats.write \
            .mode("overwrite") \
            .format("parquet") \
            .option("compression", "snappy") \
            .save(f"{output_path}/actor_stats")
        
        logger.info("Actor films processing completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error processing actor films: {str(e)}")
        raise

def main():
    """Main function to run the job with proper configuration and error handling"""
    spark = SparkSession.builder \
        .appName("ActorFilmsJob") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "2g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    try:
        # TODO: Replace with actual paths from configuration
        input_path = "path/to/input/data"
        output_path = "path/to/output/data"
        history_path = "path/to/history/data"
        
        success = process_actor_films(
            spark=spark,
            input_path=input_path,
            output_path=output_path,
            history_table_path=history_path
        )
        
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