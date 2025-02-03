import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from jobs.player_seasons_job import (
    create_player_seasons_schema,
    process_player_seasons,
    validate_data_quality
)

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing"""
    return SparkSession.builder \
        .appName("TestPlayerSeasonsJob") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

@pytest.fixture
def sample_data():
    """Create sample test data"""
    return [
        ("LeBron James", 35, "6-9", 250, "St. Vincent-St. Mary", "USA", "2003", "1", "1", 
         67.0, 25.3, 7.8, 10.2, 9.8, 3.1, 18.3, 31.5, 58.7, 49.3, 2020),
        ("Stephen Curry", 32, "6-3", 185, "Davidson", "USA", "2009", "1", "7",
         65.0, 30.1, 5.5, 5.9, 12.3, 2.0, 10.1, 32.1, 65.5, 34.2, 2020),
        (None, 25, "6-6", 200, "Unknown", "USA", "2015", "2", "31",
         50.0, 15.0, 5.0, 3.0, 5.0, 2.0, 8.0, 20.0, 55.0, 25.0, None)  # Invalid row
    ]

def test_create_player_seasons_schema(spark):
    """Test schema creation and validation"""
    schema = create_player_seasons_schema()
    
    # Verify it's a StructType
    assert isinstance(schema, StructType)
    
    # Create a test DataFrame with the schema
    test_data = [
        ("LeBron James", 35, "6-9", 250, "St. Vincent-St. Mary", "USA", "2003", "1", "1", 
         67.0, 25.3, 7.8, 10.2, 9.8, 3.1, 18.3, 31.5, 58.7, 49.3, 2020)
    ]
    
    df = spark.createDataFrame(test_data, schema)
    
    # Verify schema types
    assert df.schema["player_name"].dataType.simpleString() == "string"
    assert not df.schema["player_name"].nullable  # Should be NOT NULL
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
    assert not df.schema["season"].nullable  # Should be NOT NULL

def test_validate_data_quality(spark, sample_data):
    """Test data quality validation function"""
    schema = create_player_seasons_schema()
    df = spark.createDataFrame(sample_data, schema)
    
    # Test quality validation
    assert not validate_data_quality(df)  # Should fail due to null values
    
    # Test with clean data
    clean_data = [row for row in sample_data if row[0] is not None and row[-1] is not None]
    clean_df = spark.createDataFrame(clean_data, schema)
    assert validate_data_quality(clean_df)  # Should pass with clean data

def test_process_player_seasons(spark, sample_data, tmp_path):
    """Test the end-to-end processing logic"""
    schema = create_player_seasons_schema()
    input_df = spark.createDataFrame(sample_data, schema)
    
    # Create temporary paths for testing
    input_path = str(tmp_path / "input")
    output_path = str(tmp_path / "output")
    
    # Write test data
    input_df.write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(input_path)
    
    # Process the data
    success = process_player_seasons(spark, input_path, output_path)
    assert success  # Should succeed despite invalid rows
    
    # Read processed data
    result_df = spark.read.format("parquet").load(output_path)
    
    # Verify results
    assert result_df.count() == 2  # Should have filtered out the invalid row
    assert len(result_df.columns) == 20
    
    # Verify data quality
    assert result_df.filter("player_name is null").count() == 0
    assert result_df.filter("season is null").count() == 0
    
    # Verify specific records
    lebron_row = result_df.filter("player_name = 'LeBron James'").collect()[0]
    assert lebron_row.age == 35
    assert lebron_row.pts == 25.3
    assert lebron_row.season == 2020

def test_error_handling(spark, tmp_path):
    """Test error handling for invalid input"""
    input_path = str(tmp_path / "nonexistent")
    output_path = str(tmp_path / "output")
    
    # Test with non-existent input path
    with pytest.raises(Exception):
        process_player_seasons(spark, input_path, output_path)

def test_performance(spark, sample_data, tmp_path):
    """Test performance optimization settings"""
    # Verify Spark configurations
    assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "2"  # Test setting
    
    # Create a larger dataset for performance testing
    large_data = sample_data * 100  # Multiply data for testing
    schema = create_player_seasons_schema()
    df = spark.createDataFrame(large_data, schema)
    
    input_path = str(tmp_path / "large_input")
    output_path = str(tmp_path / "large_output")
    
    # Write test data
    df.write.mode("overwrite").format("csv").option("header", "true").save(input_path)
    
    # Process and verify it completes without error
    success = process_player_seasons(spark, input_path, output_path)
    assert success 