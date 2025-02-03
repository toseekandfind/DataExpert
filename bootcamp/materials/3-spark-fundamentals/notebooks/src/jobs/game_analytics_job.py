from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def create_spark_session():
    """Create a Spark session with the required configurations"""
    spark = SparkSession.builder \
        .appName("GameAnalytics") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .getOrCreate()
    return spark

def read_and_prepare_tables(spark: SparkSession, base_path: str):
    """
    Read all required tables and prepare them for processing
    
    Args:
        spark: SparkSession object
        base_path: Base path where data files are stored
    """
    # Read tables
    match_details = spark.read.parquet(f"{base_path}/match_details") \
        .repartitionByRange(16, "match_id")
    
    matches = spark.read.parquet(f"{base_path}/matches") \
        .repartitionByRange(16, "match_id")
    
    medal_matches_players = spark.read.parquet(f"{base_path}/medals_matches_players") \
        .repartitionByRange(16, "match_id")
    
    # These will be broadcast
    medals = spark.read.parquet(f"{base_path}/medals")
    maps = spark.read.parquet(f"{base_path}/maps")
    
    return match_details, matches, medal_matches_players, F.broadcast(medals), F.broadcast(maps)

def analyze_player_kills(match_details, matches):
    """Calculate average kills per game for each player"""
    return match_details.join(matches, "match_id") \
        .groupBy("player_id") \
        .agg(
            F.avg("kills").alias("avg_kills"),
            F.count("match_id").alias("total_games")
        ) \
        .orderBy(F.desc("avg_kills"))

def analyze_playlist_popularity(matches):
    """Determine which playlist gets played the most"""
    return matches.groupBy("playlist") \
        .agg(F.count("*").alias("times_played")) \
        .orderBy(F.desc("times_played"))

def analyze_map_popularity(matches, maps):
    """Determine which map gets played the most"""
    return matches.join(maps, "map_id") \
        .groupBy("map_name") \
        .agg(F.count("*").alias("times_played")) \
        .orderBy(F.desc("times_played"))

def analyze_killing_spree_maps(medal_matches_players, medals, matches, maps):
    """Find maps where players get the most Killing Spree medals"""
    return medal_matches_players.join(medals, "medal_id") \
        .filter(F.col("medal_name") == "Killing Spree") \
        .join(matches, "match_id") \
        .join(maps, "map_id") \
        .groupBy("map_name") \
        .agg(F.count("*").alias("spree_count")) \
        .orderBy(F.desc("spree_count"))

def optimize_partitions(df, sort_col):
    """Optimize partitions by sorting within them"""
    return df.sortWithinPartitions(sort_col)

def process_game_analytics(spark: SparkSession, input_base_path: str, output_base_path: str):
    """
    Main processing function that implements all required analytics
    
    Args:
        spark: SparkSession object
        input_base_path: Base path for input data
        output_base_path: Base path for output data
    """
    # Read and prepare all tables
    match_details, matches, medal_matches_players, medals, maps = read_and_prepare_tables(spark, input_base_path)
    
    # Perform analytics
    player_kills = analyze_player_kills(match_details, matches)
    playlist_popularity = analyze_playlist_popularity(matches)
    map_popularity = analyze_map_popularity(matches, maps)
    killing_spree_maps = analyze_killing_spree_maps(medal_matches_players, medals, matches, maps)
    
    # Optimize partitions
    player_kills_opt = optimize_partitions(player_kills, "avg_kills")
    playlist_popularity_opt = optimize_partitions(playlist_popularity, "playlist")
    map_popularity_opt = optimize_partitions(map_popularity, "map_name")
    
    # Save results
    player_kills_opt.write.mode("overwrite").parquet(f"{output_base_path}/player_kills")
    playlist_popularity_opt.write.mode("overwrite").parquet(f"{output_base_path}/playlist_popularity")
    map_popularity_opt.write.mode("overwrite").parquet(f"{output_base_path}/map_popularity")
    killing_spree_maps.write.mode("overwrite").parquet(f"{output_base_path}/killing_spree_maps")

def main():
    """Main function to run the job"""
    spark = create_spark_session()
    
    try:
        # TODO: Replace with actual paths
        input_base_path = "path/to/input/data"
        output_base_path = "path/to/output/data"
        
        process_game_analytics(spark, input_base_path, output_base_path)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 