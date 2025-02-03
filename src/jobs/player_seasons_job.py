from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def create_player_seasons_schema():
    """
    Creates and returns the schema for player_seasons table
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

def process_player_seasons(spark: SparkSession, input_path: str, output_path: str):
    """
    Process player_seasons data using PySpark
    
    Args:
        spark: SparkSession object
        input_path: Path to input data
        output_path: Path where processed data should be saved
    """
    # Define schema
    schema = create_player_seasons_schema()
    
    # Read data with schema
    df = spark.read.schema(schema).format("csv").option("header", "true").load(input_path)
    
    # Ensure data quality - remove any nulls in required fields
    df_clean = df.dropna(subset=["player_name", "season"])
    
    # Write the processed data
    df_clean.write.mode("overwrite").format("parquet").save(output_path)

def main():
    """Main function to run the job"""
    spark = SparkSession.builder \
        .appName("PlayerSeasonsJob") \
        .getOrCreate()
    
    try:
        # TODO: Replace with actual input/output paths
        input_path = "path/to/input/data"
        output_path = "path/to/output/data"
        
        process_player_seasons(spark, input_path, output_path)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 