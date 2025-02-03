import pytest
from pyspark.sql import SparkSession
from jobs.player_seasons_job import (
    create_player_seasons_schema,
    process_player_seasons,
    analyze_player_performance
)

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing"""
    return SparkSession.builder \
        .appName("TestPlayerSeasonsJob") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture
def sample_player_seasons(spark):
    """Create sample player seasons data"""
    test_data = [
        # player with >= 20 games
        ("LeBron James", 35, "6-9", 250, "St. Vincent-St. Mary", "USA", "2003", "1", "1", 
         67.0, 25.3, 7.8, 10.2, 9.8, 3.1, 18.3, 31.5, 58.7, 49.3, 2020),
        # player with >= 20 games
        ("Stephen Curry", 32, "6-3", 185, "Davidson", "USA", "2009", "1", "7",
         65.0, 30.1, 5.5, 5.9, 12.3, 2.0, 10.1, 32.1, 65.5, 34.2, 2020),
        # player with < 20 games (should be filtered from stats)
        ("Rookie Player", 22, "6-6", 200, "Kentucky", "USA", "2020", "1", "15",
         15.0, 12.0, 4.0, 3.0, 5.0, 1.5, 8.0, 20.0, 52.0, 25.0, 2020),
        # invalid player (should be filtered from raw data)
        (None, 25, "6-6", 200, "Unknown", "USA", "2015", "2", "31",
         50.0, 15.0, 5.0, 3.0, 5.0, 2.0, 8.0, 20.0, 55.0, 25.0, None)
    ]
    return spark.createDataFrame(test_data, create_player_seasons_schema())

def test_create_player_seasons_schema(spark):
    """Test schema creation"""
    schema = create_player_seasons_schema()
    
    # Create a test DataFrame with the schema
    test_data = [
        ("LeBron James", 35, "6-9", 250, "St. Vincent-St. Mary", "USA", "2003", "1", "1", 
         67.0, 25.3, 7.8, 10.2, 9.8, 3.1, 18.3, 31.5, 58.7, 49.3, 2020),
        ("Stephen Curry", 32, "6-3", 185, "Davidson", "USA", "2009", "1", "7",
         65.0, 30.1, 5.5, 5.9, 12.3, 2.0, 10.1, 32.1, 65.5, 34.2, 2020)
    ]
    
    df = spark.createDataFrame(test_data, schema)
    
    # Verify schema types
    assert df.schema["player_name"].dataType.simpleString() == "string"
    assert df.schema["age"].dataType.simpleString() == "int"
    assert df.schema["height"].dataType.simpleString() == "string"
    assert df.schema["weight"].dataType.simpleString() == "int"
    assert df.schema["college"].dataType.simpleString() == "string"
    assert df.schema["country"].dataType.simpleString() == "string"
    assert df.schema["draft_year"].dataType.simpleString() == "string"
    assert df.schema["draft_round"].dataType.simpleString() == "string"
    assert df.schema["draft_number"].dataType.simpleString() == "string"
    assert df.schema["gp"].dataType.simpleString() == "float"
    assert df.schema["pts"].dataType.simpleString() == "float"
    assert df.schema["season"].dataType.simpleString() == "int"

def test_analyze_player_performance(spark, sample_player_seasons):
    """Test player performance analysis"""
    result = analyze_player_performance(sample_player_seasons).collect()
    
    assert len(result) == 2  # Only players with >= 20 games
    
    # Check Curry's stats (should be first with higher PPG)
    curry = result[0]
    assert curry["player_name"] == "Stephen Curry"
    assert curry["ppg"] == 0.46  # 30.1 / 65.0 rounded to 2 decimals
    assert curry["rpg"] == 0.08  # 5.5 / 65.0 rounded to 2 decimals
    assert curry["apg"] == 0.09  # 5.9 / 65.0 rounded to 2 decimals
    assert curry["true_shooting_pct"] == 65.5
    
    # Check LeBron's stats
    lebron = result[1]
    assert lebron["player_name"] == "LeBron James"
    assert lebron["ppg"] == 0.38  # 25.3 / 67.0 rounded to 2 decimals
    assert lebron["rpg"] == 0.12  # 7.8 / 67.0 rounded to 2 decimals
    assert lebron["apg"] == 0.15  # 10.2 / 67.0 rounded to 2 decimals
    assert lebron["true_shooting_pct"] == 58.7

def test_process_player_seasons(spark, sample_player_seasons, tmp_path):
    """Test the processing logic"""
    # Create temporary paths for testing
    input_path = str(tmp_path / "input")
    output_path = str(tmp_path / "output")
    
    # Write test data
    sample_player_seasons.write.mode("overwrite").format("csv").option("header", "true").save(input_path)
    
    # Process the data
    process_player_seasons(spark, input_path, output_path)
    
    # Read processed data
    raw_df = spark.read.format("parquet").load(f"{output_path}/raw")
    stats_df = spark.read.format("parquet").load(f"{output_path}/player_stats")
    
    # Verify raw data results
    assert raw_df.count() == 3  # Should have filtered out the invalid row
    assert len(raw_df.columns) == 20
    assert raw_df.filter("player_name is null").count() == 0
    assert raw_df.filter("season is null").count() == 0
    
    # Verify stats results
    assert stats_df.count() == 2  # Only players with >= 20 games
    assert len(stats_df.columns) == 6  # player_name, season, ppg, rpg, apg, true_shooting_pct 