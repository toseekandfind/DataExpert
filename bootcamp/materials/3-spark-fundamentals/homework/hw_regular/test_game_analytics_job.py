import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import os
import shutil
from game_analytics_job import (
    create_spark_session,
    prepare_tables,
    analyze_player_kills,
    analyze_playlist_popularity,
    analyze_map_popularity,
    analyze_killing_spree_maps,
    explore_partitioning_strategies,
    measure_data_size,
    create_bucketed_table,
    write_optimized_output
)

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing with required configurations"""
    return create_spark_session("TestGameAnalytics")

@pytest.fixture
def sample_data(spark, tmp_path):
    """Create sample data for testing"""
    # Create sample match details with more data for better testing
    match_details_data = [
        (1, "player1", 10, 5, 2),  # match_id, player_id, kills, deaths, assists
        (1, "player2", 8, 6, 3),
        (2, "player1", 15, 4, 1),
        (2, "player3", 12, 7, 2),
        (3, "player1", 20, 3, 4),  # Additional data for better statistics
        (3, "player2", 18, 5, 2),
        (4, "player3", 25, 8, 1)
    ]
    match_details = spark.createDataFrame(
        match_details_data,
        ["match_id", "player_id", "kills", "deaths", "assists"]
    )
    
    # Create sample matches with varied playlists and maps
    matches_data = [
        (1, "map1", "playlist1", "2024-01-01"),
        (2, "map2", "playlist2", "2024-01-02"),
        (3, "map1", "playlist1", "2024-01-03"),
        (4, "map3", "playlist3", "2024-01-04")
    ]
    matches = spark.createDataFrame(
        matches_data,
        ["match_id", "map_id", "playlist_name", "match_date"]
    )
    
    # Create sample medals with more variety
    medals_data = [
        ("medal1", "Killing Spree"),
        ("medal2", "Double Kill"),
        ("medal3", "Triple Kill"),
        ("medal4", "Killing Spree Master"),
        ("medal5", "Killing Frenzy")
    ]
    medals = spark.createDataFrame(
        medals_data,
        ["medal_id", "medal_name"]
    )
    
    # Create sample medal matches players with more sprees
    medal_matches_data = [
        (1, "player1", "medal1"),
        (1, "player2", "medal1"),
        (2, "player1", "medal1"),
        (2, "player3", "medal2"),
        (3, "player1", "medal4"),
        (3, "player2", "medal1"),
        (4, "player3", "medal5")
    ]
    medals_matches_players = spark.createDataFrame(
        medal_matches_data,
        ["match_id", "player_id", "medal_id"]
    )
    
    # Create sample maps with descriptions
    maps_data = [
        ("map1", "Arena Map"),
        ("map2", "Forest Map"),
        ("map3", "Desert Map")
    ]
    maps = spark.createDataFrame(
        maps_data,
        ["map_id", "map_name"]
    )
    
    # Write sample data
    base_path = str(tmp_path)
    match_details.write.parquet(f"{base_path}/match_details")
    matches.write.parquet(f"{base_path}/matches")
    medals.write.parquet(f"{base_path}/medals")
    medals_matches_players.write.parquet(f"{base_path}/medals_matches_players")
    maps.write.parquet(f"{base_path}/maps")
    
    return {
        "base_path": base_path,
        "expected_counts": {
            "match_details": len(match_details_data),
            "matches": len(matches_data),
            "medals": len(medals_data),
            "medals_matches_players": len(medal_matches_data),
            "maps": len(maps_data)
        }
    }

def test_create_bucketed_table(spark, sample_data, tmp_path):
    """Test creation and verification of bucketed tables"""
    df = spark.read.parquet(f"{sample_data['base_path']}/match_details")
    
    # Create bucketed table
    create_bucketed_table(
        df=df,
        table_name="test_bucketed",
        bucket_cols=["match_id"],
        num_buckets=16,
        sort_cols=["match_id"]
    )
    
    # Verify table exists and is properly bucketed
    table = spark.table("test_bucketed")
    plan = table._jdf.queryExecution().analyzed().toString().lower()
    
    # Verify bucketing properties
    assert "bucket" in plan
    assert "numbuckets=16" in plan.replace(" ", "")
    assert "bucketby" in plan.replace(" ", "")
    assert table.count() == sample_data["expected_counts"]["match_details"]
    
    # Clean up
    spark.sql("DROP TABLE IF EXISTS test_bucketed")

def test_prepare_tables_with_bucket_join(spark, sample_data):
    """Test preparation of tables with proper bucket join implementation"""
    tables = prepare_tables(spark, sample_data["base_path"])
    
    # Verify all tables are present
    assert all(name in tables for name in [
        "match_details", "matches", "medals_matches_players", "medals", "maps"
    ])
    
    # Verify bucketing for large tables
    for table_name in ["match_details", "matches", "medals_matches_players"]:
        table = tables[table_name]
        plan = table._jdf.queryExecution().analyzed().toString().lower()
        
        # Verify bucket join properties
        assert "bucket" in plan
        assert "numbuckets=16" in plan.replace(" ", "")
        assert table.count() == sample_data["expected_counts"][table_name]
        
        # Verify table is properly bucketed by match_id
        bucket_columns = [col.name for col in table.schema if "match_id" in col.name.lower()]
        assert len(bucket_columns) > 0

def test_broadcast_join_implementation(spark, sample_data):
    """Test proper implementation of broadcast joins"""
    tables = prepare_tables(spark, sample_data["base_path"])
    
    # Test map popularity analysis (should use broadcast join)
    result = analyze_map_popularity(tables)
    plan = result._jdf.queryExecution().executedPlan().toString().lower()
    
    # Verify broadcast join is used for maps table
    assert "broadcast" in plan
    assert "broadcastexchange" in plan.replace(" ", "")
    
    # Verify join maintains data integrity
    result_data = result.collect()
    assert len(result_data) == sample_data["expected_counts"]["maps"]
    
    # Verify most played map is correct
    most_played = result_data[0]
    assert most_played["map_id"] == "map1"  # Based on our sample data
    assert most_played["total_matches"] == 2  # map1 appears twice

def test_mixed_join_strategy(spark, sample_data):
    """Test combination of bucket and broadcast joins"""
    tables = prepare_tables(spark, sample_data["base_path"])
    
    # Analyze killing sprees (uses both join types)
    result = analyze_killing_spree_maps(tables)
    plan = result._jdf.queryExecution().executedPlan().toString().lower()
    
    # Verify both join types are used
    assert "bucket" in plan  # For match_id joins
    assert "broadcast" in plan  # For medals and maps joins
    assert "broadcastexchange" in plan.replace(" ", "")
    
    # Verify results maintain data integrity
    result_data = result.collect()
    assert len(result_data) > 0
    
    # Verify spree calculations
    top_spree_map = result_data[0]
    assert "total_sprees" in top_spree_map
    assert "sprees_per_match" in top_spree_map
    assert float(top_spree_map["sprees_per_match"]) > 0

def test_partitioning_strategies(spark, sample_data, tmp_path):
    """Test comprehensive partitioning strategy exploration"""
    df = spark.read.parquet(f"{sample_data['base_path']}/matches")
    test_path = str(tmp_path / "partitioning_test")
    
    # Test strategy exploration
    best_strategy, partition_cols = explore_partitioning_strategies(
        df=df,
        name="test_partitioning",
        base_path=test_path
    )
    
    # Verify strategy selection
    assert isinstance(best_strategy, str)
    assert isinstance(partition_cols, list)
    
    # Test writing with optimized partitioning
    output_path = str(tmp_path / "optimized_output")
    write_optimized_output(
        df=df,
        name="test_output",
        output_path=output_path,
        temp_path=test_path
    )
    
    # Verify optimized output
    assert os.path.exists(output_path)
    if partition_cols:
        for col in partition_cols:
            partition_path = f"{output_path}/test_output"
            assert any(col in d for d in os.listdir(partition_path))

def test_error_handling(spark, tmp_path):
    """Test error handling for invalid inputs and edge cases"""
    # Test with non-existent path
    invalid_path = str(tmp_path / "nonexistent")
    with pytest.raises(Exception) as exc_info:
        prepare_tables(spark, invalid_path)
    assert "does not exist" in str(exc_info.value).lower()
    
    # Test with invalid data structure
    invalid_df = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(Exception) as exc_info:
        create_bucketed_table(invalid_df, "test_invalid", ["nonexistent_col"], 16)
    assert "column" in str(exc_info.value).lower()
    
    # Test with invalid bucket number
    with pytest.raises(Exception) as exc_info:
        create_bucketed_table(invalid_df, "test_invalid", ["id"], 0)
    assert "bucket" in str(exc_info.value).lower()

def test_cleanup(spark):
    """Test cleanup of temporary resources"""
    # Create test resources
    df = spark.createDataFrame([(1,)], ["id"])
    create_bucketed_table(df, "test_cleanup", ["id"], 2)
    
    # Verify resources exist
    assert "test_cleanup" in spark.catalog.listTables()
    
    # Clean up
    spark.sql("DROP TABLE IF EXISTS test_cleanup")
    
    # Verify cleanup
    assert "test_cleanup" not in spark.catalog.listTables() 