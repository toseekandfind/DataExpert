from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def create_actor_films_schema():
    """
    Creates and returns the schema for actor_films table
    """
    return StructType([
        StructField("actor", StringType(), True),
        StructField("actor_id", StringType(), True),
        StructField("film", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("votes", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("film_id", StringType(), True)
    ])

def process_actor_films(spark: SparkSession, input_path: str, output_path: str):
    """
    Process actor_films data using PySpark
    
    Args:
        spark: SparkSession object
        input_path: Path to input data
        output_path: Path where processed data should be saved
    """
    # Define schema
    schema = create_actor_films_schema()
    
    # Read data with schema
    df = spark.read.schema(schema).format("csv").option("header", "true").load(input_path)
    
    # Ensure data quality - remove any nulls in key fields
    df_clean = df.dropna(subset=["actor_id", "film_id"])
    
    # Write the processed data
    df_clean.write.mode("overwrite").format("parquet").save(output_path)

def main():
    """Main function to run the job"""
    spark = SparkSession.builder \
        .appName("ActorFilmsJob") \
        .getOrCreate()
    
    try:
        # TODO: Replace with actual input/output paths
        input_path = "path/to/input/data"
        output_path = "path/to/output/data"
        
        process_actor_films(spark, input_path, output_path)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 