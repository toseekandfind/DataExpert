from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging
from typing import Dict, List, Tuple
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session(app_name: str = "GameAnalyticsJob") -> SparkSession:
    """
    Create a SparkSession with optimized settings for game analytics
    
    Key configurations:
    1. Disabled auto broadcast joins as per requirement
    2. Enabled bucketing for proper bucket joins
    3. Enabled adaptive query execution for better performance
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \  # Requirement 1: Disable automatic broadcast
        .config("spark.sql.sources.bucketing.enabled", "true") \  # Enable bucketing
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .enableHiveSupport() \  # Required for bucket joins
        .getOrCreate()

def create_bucketed_table(df, table_name: str, bucket_cols: List[str], num_buckets: int, sort_cols: List[str] = None):
    """
    Create a bucketed table for efficient joins
    
    Implementation details:
    1. Uses bucketBy for proper bucket join support
    2. Optionally sorts within buckets for better performance
    3. Saves as a managed table for bucket join optimization
    
    Args:
        df: DataFrame to bucket
        table_name: Name for the bucketed table
        bucket_cols: Columns to bucket by
        num_buckets: Number of buckets to create
        sort_cols: Optional columns to sort by within buckets
    """
    writer = df.write.mode("overwrite").bucketBy(num_buckets, *bucket_cols)
    if sort_cols:
        writer = writer.sortBy(*sort_cols)
    writer.saveAsTable(table_name)

def prepare_tables(spark: SparkSession, input_path: str) -> Dict:
    """
    Prepare tables with proper bucketing and broadcasting
    
    Implementation details:
    1. Creates bucketed tables for large tables using match_id
    2. Uses 16 buckets as specified in requirements
    3. Broadcasts small dimension tables
    4. Saves tables in a way that enables bucket join optimization
    
    Args:
        spark: SparkSession object
        input_path: Base path for input data
        
    Returns:
        Dict: Dictionary containing prepared DataFrames
    """
    logger.info("Preparing tables with bucketing and broadcasting...")
    
    # Read source tables
    match_details = spark.read.parquet(f"{input_path}/match_details")
    matches = spark.read.parquet(f"{input_path}/matches")
    medals_matches_players = spark.read.parquet(f"{input_path}/medals_matches_players")
    medals = spark.read.parquet(f"{input_path}/medals")
    maps = spark.read.parquet(f"{input_path}/maps")
    
    # Requirement 2: Explicitly broadcast small dimension tables
    medals_broadcast = F.broadcast(medals)
    maps_broadcast = F.broadcast(maps)
    
    # Requirement 3: Implement bucket join with 16 buckets
    # Create bucketed tables for large tables
    logger.info("Creating bucketed tables for join optimization...")
    
    # Create bucketed tables with proper bucket join support
    create_bucketed_table(
        df=match_details,
        table_name="match_details_bucketed",
        bucket_cols=["match_id"],
        num_buckets=16,
        sort_cols=["match_id"]
    )
    
    create_bucketed_table(
        df=matches,
        table_name="matches_bucketed",
        bucket_cols=["match_id"],
        num_buckets=16,
        sort_cols=["match_id"]
    )
    
    create_bucketed_table(
        df=medals_matches_players,
        table_name="medals_matches_players_bucketed",
        bucket_cols=["match_id"],
        num_buckets=16,
        sort_cols=["match_id"]
    )
    
    logger.info("Bucketed tables created successfully")
    
    # Return dictionary with bucketed and broadcast tables
    return {
        "match_details": spark.table("match_details_bucketed"),
        "matches": spark.table("matches_bucketed"),
        "medals_matches_players": spark.table("medals_matches_players_bucketed"),
        "medals": medals_broadcast,
        "maps": maps_broadcast
    }

def measure_data_size(df, path: str) -> int:
    """
    Measure the size of data when written with specific configuration
    
    Args:
        df: DataFrame to measure
        path: Path to write data temporarily
        
    Returns:
        int: Size in bytes
    """
    # Write data
    df.write.mode("overwrite").format("parquet").save(path)
    
    # Calculate size
    total_size = sum(
        os.path.getsize(os.path.join(dirpath, filename))
        for dirpath, _, filenames in os.walk(path)
        for filename in filenames
        if filename.endswith(".parquet")
    )
    
    return total_size

def explore_partitioning_strategies(df, name: str, base_path: str) -> Tuple[str, List[str]]:
    """
    Explore different partitioning strategies and measure their impact
    
    Strategies explored:
    1. No partitioning (baseline)
    2. Single column partitioning
    3. Multiple column partitioning
    4. Partitioning with sorting
    
    Args:
        df: DataFrame to analyze
        name: Name of the analysis
        base_path: Base path for temporary writes
        
    Returns:
        Tuple[str, List[str]]: Best strategy and partition columns
    """
    logger.info(f"Exploring partitioning strategies for {name}...")
    
    strategies = {
        "baseline": ([], []),  # No partitioning
        "single_partition": (["map_id"], ["map_id"]),  # Single column
        "multi_partition": (["map_id", "playlist_name"], ["map_id", "playlist_name"]),  # Multiple columns
        "sorted_partition": (["map_id"], ["map_id", "total_matches"])  # Partitioned and sorted
    }
    
    results = {}
    
    for strategy, (partition_cols, sort_cols) in strategies.items():
        test_path = f"{base_path}/test_{strategy}"
        
        # Apply strategy
        test_df = df
        if partition_cols:
            test_df = test_df.repartition(*partition_cols)
        if sort_cols:
            test_df = test_df.sortWithinPartitions(*sort_cols)
        
        # Measure size
        size = measure_data_size(test_df, test_path)
        results[strategy] = (size, partition_cols)
        
        logger.info(f"Strategy: {strategy}, Size: {size} bytes")
    
    # Find best strategy
    best_strategy = min(results.items(), key=lambda x: x[1][0])
    logger.info(f"Best strategy for {name}: {best_strategy[0]}")
    
    return best_strategy[0], best_strategy[1][1]

def write_optimized_output(df: DataFrame, name: str, output_path: str, temp_path: str):
    """
    Write DataFrame with optimized partitioning based on exploration
    
    Strategy:
    1. Explore different partitioning strategies
    2. Choose best strategy based on data size
    3. Apply optimal strategy and write final output
    
    Args:
        df: DataFrame to write
        name: Name of the analysis
        output_path: Final output path
        temp_path: Path for temporary exploration
    """
    logger.info(f"Optimizing output for {name}...")
    
    # Explore strategies and get best one
    best_strategy, partition_cols = explore_partitioning_strategies(df, name, temp_path)
    
    # Apply best strategy
    optimized_df = df
    if partition_cols:
        optimized_df = df.repartition(*partition_cols)
    optimized_df = optimized_df.sortWithinPartitions(*(partition_cols or df.columns))
    
    # Write final output
    writer = optimized_df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(f"{output_path}/{name}")
    
    logger.info(f"Wrote optimized output for {name} using {best_strategy} strategy")

def analyze_player_kills(tables: Dict) -> DataFrame:
    """
    Analyze which player averages the most kills per game
    Using bucket join between match_details and matches
    
    Args:
        tables: Dictionary containing DataFrames
        
    Returns:
        DataFrame: Player kill statistics
    """
    logger.info("Analyzing player kill statistics...")
    
    return tables["match_details"] \
        .join(tables["matches"], "match_id") \  # Uses bucket join
        .groupBy("player_id") \
        .agg(
            F.count("match_id").alias("total_matches"),
            F.sum("kills").alias("total_kills"),
            F.avg("kills").alias("avg_kills_per_game")
        ) \
        .where("total_matches >= 10") \
        .orderBy(F.desc("avg_kills_per_game"))

def analyze_playlist_popularity(tables: Dict) -> DataFrame:
    """
    Analyze which playlist gets played the most
    
    Args:
        tables: Dictionary containing DataFrames
        
    Returns:
        DataFrame: Playlist popularity statistics
    """
    logger.info("Analyzing playlist popularity...")
    
    return tables["matches"] \
        .groupBy("playlist_name") \
        .agg(F.count("*").alias("total_matches")) \
        .orderBy(F.desc("total_matches"))

def analyze_map_popularity(tables: Dict) -> DataFrame:
    """
    Analyze which map gets played the most
    Using broadcast join with maps table
    
    Args:
        tables: Dictionary containing DataFrames
        
    Returns:
        DataFrame: Map popularity statistics
    """
    logger.info("Analyzing map popularity...")
    
    return tables["matches"] \
        .join(tables["maps"], "map_id") \  # Uses broadcast join
        .groupBy("map_id", "map_name") \
        .agg(F.count("*").alias("total_matches")) \
        .orderBy(F.desc("total_matches"))

def analyze_killing_spree_maps(tables: Dict) -> DataFrame:
    """
    Analyze which map players get the most Killing Spree medals on
    Using bucket join and broadcast join
    
    Args:
        tables: Dictionary containing DataFrames
        
    Returns:
        DataFrame: Map killing spree statistics
    """
    logger.info("Analyzing killing spree medals by map...")
    
    return tables["medals_matches_players"] \
        .join(tables["medals"], "medal_id") \  # Uses broadcast join
        .filter(F.lower(F.col("medal_name")).like("%killing spree%")) \
        .join(tables["matches"], "match_id") \  # Uses bucket join
        .join(tables["maps"], "map_id") \  # Uses broadcast join
        .groupBy("map_id", "map_name") \
        .agg(
            F.count("*").alias("total_sprees"),
            F.countDistinct("match_id").alias("total_matches"),
            F.expr("count(*) / count(distinct match_id)").alias("sprees_per_match")
        ) \
        .orderBy(F.desc("sprees_per_match"))

def process_game_analytics(spark: SparkSession, input_path: str, output_path: str):
    """
    Main processing function for game analytics
    
    Implementation:
    1. Uses bucket joins for large tables
    2. Uses broadcast joins for dimension tables
    3. Explores and applies optimal partitioning strategies
    
    Args:
        spark: SparkSession object
        input_path: Path to input data
        output_path: Path to write output data
    """
    try:
        logger.info("Starting game analytics processing...")
        
        # Create temporary path for strategy exploration
        temp_path = f"{output_path}/temp_exploration"
        
        # Prepare tables with bucketing and broadcasting
        tables = prepare_tables(spark, input_path)
        
        # Run analyses
        analyses = {
            "player_kills": analyze_player_kills(tables),
            "playlist_popularity": analyze_playlist_popularity(tables),
            "map_popularity": analyze_map_popularity(tables),
            "killing_spree_maps": analyze_killing_spree_maps(tables)
        }
        
        # Process each analysis with optimized output
        for name, df in analyses.items():
            write_optimized_output(df, name, output_path, temp_path)
        
        logger.info("Game analytics processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error processing game analytics: {str(e)}")
        raise
    finally:
        # Clean up
        logger.info("Cleaning up temporary resources...")
        spark.sql("DROP TABLE IF EXISTS match_details_bucketed")
        spark.sql("DROP TABLE IF EXISTS matches_bucketed")
        spark.sql("DROP TABLE IF EXISTS medals_matches_players_bucketed")
        
        # Clean up temporary exploration directory
        if os.path.exists(temp_path):
            import shutil
            shutil.rmtree(temp_path)

def main():
    """Main function to run the game analytics job"""
    spark = create_spark_session()
    
    try:
        # TODO: Replace with actual paths from configuration
        input_path = "path/to/input/data"
        output_path = "path/to/output/data"
        
        process_game_analytics(spark, input_path, output_path)
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 