from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

"""
Approaches to Working with Spark:

1. DataFrame API with StructType (Our Chosen Approach):
   - Provides schema validation at read time
   - Offers type safety without the verbosity of Datasets
   - Example:
   ```python
   schema = StructType([
       StructField("player_name", StringType(), False),  # NOT NULL
       StructField("season", IntegerType(), False)       # NOT NULL
   ])
   df = spark.read.schema(schema).csv("path")
   df.filter(F.col("gp") >= 20)
   ```

2. SparkSQL with Temp Views:
   - More familiar for SQL developers
   - Less type safety but more flexible for complex queries
   - Example:
   ```python
   df.createOrReplaceTempView("player_seasons")
   result = spark.sql('''
       SELECT player_name, season,
              ROUND(pts / gp, 2) as ppg,
              ROUND(reb / gp, 2) as rpg
       FROM player_seasons
       WHERE gp >= 20
       ORDER BY ppg DESC
   ''')
   ```

3. Raw DataFrame without Schema:
   - Relies on schema inference
   - Less type safety, potential runtime errors
   - Example:
   ```python
   df = spark.read.csv("path")
   df.join(other_df, "player_name")  # No compile-time type checking
   ```

Why We Chose StructType Approach:
1. Schema Validation: 
   - Enforces NOT NULL constraints (player_name, season)
   - Validates data types at read time (Integer for age, Float for stats)
   - Catches data quality issues early in the pipeline

2. Type Safety:
   - Prevents runtime errors from type mismatches
   - Makes column references explicit and checked
   - Enables better error messages

3. Performance:
   - Avoids costly schema inference
   - Optimizes memory usage with proper type definitions
   - Enables better query planning

4. IDE Support:
   - Better code completion
   - Static analysis capabilities
   - Easier refactoring

5. Maintainability:
   - Schema changes are explicit and version controlled
   - Self-documenting code
   - Easier to track schema evolution

Trade-offs:
- More verbose initial setup compared to schema inference
- Less flexible than raw SQL for complex ad-hoc queries
- Requires more Python code compared to SQL approach

Alternative Approach (SparkSQL):
```sql
-- The same logic could be written in pure SQL:
CREATE TEMPORARY VIEW player_seasons (
    player_name STRING NOT NULL,
    age INT,
    gp FLOAT,
    pts FLOAT,
    -- ... other columns
    season INT NOT NULL
);

SELECT 
    player_name,
    season,
    ROUND(pts / gp, 2) as ppg,
    ROUND(reb / gp, 2) as rpg,
    ROUND(ast / gp, 2) as apg,
    ts_pct as true_shooting_pct
FROM player_seasons
WHERE gp >= 20
ORDER BY ppg DESC;
```

Our StructType approach provides better type safety and maintainability while
still allowing us to express complex transformations clearly.
"""

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

def analyze_player_performance(df):
    """
    Analyze player performance metrics using SparkSQL equivalent of the PostgreSQL query
    """
    return df.filter(F.col("gp") >= 20) \
        .select(
            "player_name",
            "season",
            F.round(F.col("pts") / F.col("gp"), 2).alias("ppg"),
            F.round(F.col("reb") / F.col("gp"), 2).alias("rpg"),
            F.round(F.col("ast") / F.col("gp"), 2).alias("apg"),
            F.col("ts_pct").alias("true_shooting_pct")
        ) \
        .orderBy(F.desc("ppg"))

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
    
    # Analyze player performance
    player_stats = analyze_player_performance(df_clean)
    
    # Write the processed data
    df_clean.write.mode("overwrite").format("parquet").save(f"{output_path}/raw")
    player_stats.write.mode("overwrite").format("parquet").save(f"{output_path}/player_stats")

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