import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from jobs.actor_films_job import (
    create_actor_films_schema,
    create_actor_history_scd_schema,
    process_actor_films,
    analyze_actor_statistics,
    validate_data_quality
)

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing with optimized settings"""
    return SparkSession.builder \
        .appName("TestActorFilmsJob") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

@pytest.fixture
def sample_data():
    """Create sample actor films data"""
    current_time = datetime.now()
    return [
        ("Tom Hanks", "th123", "Forrest Gump", 1994, 1000000, 8.8, "fg456", current_time),
        ("Tom Hanks", "th123", "Cast Away", 2000, 500000, 8.5, "ca789", current_time),
        ("Morgan Freeman", "mf789", "Shawshank Redemption", 1994, 2000000, 9.3, "sr789", current_time),
        ("Morgan Freeman", "mf789", "Se7en", 1995, 1000000, 8.7, "se123", current_time),
        (None, None, "Invalid Movie", 2000, 100, 5.0, None, current_time)  # Invalid row
    ]

@pytest.fixture
def sample_history_data():
    """Create sample actor history data for SCD testing"""
    past_time = datetime.now() - timedelta(days=1)
    return [
        # Old records for Tom Hanks
        ("th123", "Tom Hanks", 1, 8.8, 1000000, past_time, None, "Y"),
        # Old records for Morgan Freeman
        ("mf789", "Morgan Freeman", 1, 9.3, 2000000, past_time, None, "Y")
    ]

def test_create_actor_films_schema(spark):
    """Test schema creation and validation with nullability checks"""
    schema = create_actor_films_schema()
    
    # Verify it's a StructType
    assert isinstance(schema, StructType)
    
    # Create a test DataFrame with the schema
    current_time = datetime.now()
    test_data = [
        ("Tom Hanks", "th123", "Forrest Gump", 1994, 1000000, 8.8, "fg456", current_time)
    ]
    
    df = spark.createDataFrame(test_data, schema)
    
    # Verify schema types and nullability
    assert df.schema["actor"].dataType.simpleString() == "string"
    assert not df.schema["actor"].nullable  # Should be NOT NULL
    assert df.schema["actor_id"].dataType.simpleString() == "string"
    assert not df.schema["actor_id"].nullable  # Should be NOT NULL
    assert df.schema["film"].dataType.simpleString() == "string"
    assert df.schema["year"].dataType.simpleString() == "int"
    assert df.schema["votes"].dataType.simpleString() == "int"
    assert df.schema["rating"].dataType.simpleString() == "float"
    assert df.schema["film_id"].dataType.simpleString() == "string"
    assert not df.schema["film_id"].nullable  # Should be NOT NULL
    assert df.schema["last_updated"].dataType.simpleString() == "timestamp"

def test_create_actor_history_scd_schema(spark):
    """Test SCD history schema creation and validation"""
    schema = create_actor_history_scd_schema()
    
    # Verify it's a StructType
    assert isinstance(schema, StructType)
    
    # Create a test DataFrame with the schema
    current_time = datetime.now()
    test_data = [
        ("th123", "Tom Hanks", 2, 8.7, 1500000, current_time, None, "Y")
    ]
    
    df = spark.createDataFrame(test_data, schema)
    
    # Verify schema types and nullability
    assert df.schema["actor_id"].dataType.simpleString() == "string"
    assert not df.schema["actor_id"].nullable
    assert df.schema["actor"].dataType.simpleString() == "string"
    assert not df.schema["actor"].nullable
    assert df.schema["valid_from"].dataType.simpleString() == "timestamp"
    assert not df.schema["valid_from"].nullable
    assert df.schema["is_current"].dataType.simpleString() == "string"
    assert not df.schema["is_current"].nullable

def test_validate_data_quality(spark, sample_data):
    """Test comprehensive data quality validation"""
    schema = create_actor_films_schema()
    df = spark.createDataFrame(sample_data, schema)
    
    # Test quality validation with invalid data
    assert not validate_data_quality(df)  # Should fail due to null values
    
    # Test with clean data
    clean_data = [row for row in sample_data if all(x is not None for x in (row[0], row[1], row[6]))]
    clean_df = spark.createDataFrame(clean_data, schema)
    assert validate_data_quality(clean_df)  # Should pass with clean data
    
    # Test with invalid year
    invalid_year_data = clean_data + [
        ("Test Actor", "ta123", "Future Movie", 2050, 1000, 8.0, "fm123", datetime.now())
    ]
    invalid_year_df = spark.createDataFrame(invalid_year_data, schema)
    assert not validate_data_quality(invalid_year_df)  # Should fail due to future year
    
    # Test with invalid rating
    invalid_rating_data = clean_data + [
        ("Test Actor", "ta123", "Bad Rating", 2020, 1000, 11.0, "br123", datetime.now())
    ]
    invalid_rating_df = spark.createDataFrame(invalid_rating_data, schema)
    assert not validate_data_quality(invalid_rating_df)  # Should fail due to invalid rating
    
    # Test with duplicates
    duplicate_data = clean_data + [clean_data[0]]
    duplicate_df = spark.createDataFrame(duplicate_data, schema)
    assert not validate_data_quality(duplicate_df)  # Should fail due to duplicates

def test_analyze_actor_statistics(spark, sample_data):
    """Test actor statistics analysis with detailed validation"""
    schema = create_actor_films_schema()
    clean_data = [row for row in sample_data if all(x is not None for x in (row[0], row[1], row[6]))]
    df = spark.createDataFrame(clean_data, schema)
    
    result = analyze_actor_statistics(df).collect()
    
    assert len(result) == 2  # Both actors have multiple films
    
    # Check Tom Hanks stats
    tom = [r for r in result if r["actor"] == "Tom Hanks"][0]
    assert tom["num_films"] == 2
    assert round(tom["avg_rating"], 1) == 8.7  # (8.8 + 8.5) / 2
    assert tom["total_votes"] == 1500000  # 1000000 + 500000
    assert tom["first_film_year"] == 1994
    assert tom["last_film_year"] == 2000
    assert tom["career_span_years"] == 6
    assert round(tom["films_per_year"], 2) == round(2 / 6, 2)
    assert len(tom["films"]) == 2
    
    # Check Morgan Freeman stats
    morgan = [r for r in result if r["actor"] == "Morgan Freeman"][0]
    assert morgan["num_films"] == 2
    assert round(morgan["avg_rating"], 1) == 9.0  # (9.3 + 8.7) / 2
    assert morgan["total_votes"] == 3000000  # 2000000 + 1000000
    assert morgan["first_film_year"] == 1994
    assert morgan["last_film_year"] == 1995
    assert morgan["career_span_years"] == 1
    assert round(morgan["films_per_year"], 2) == 2.0
    assert len(morgan["films"]) == 2

def test_process_actor_films_with_scd(spark, sample_data, sample_history_data, tmp_path):
    """Test the end-to-end processing logic with SCD implementation"""
    # Set up schemas
    data_schema = create_actor_films_schema()
    history_schema = create_actor_history_scd_schema()
    
    # Create input DataFrame
    input_df = spark.createDataFrame(sample_data, data_schema)
    history_df = spark.createDataFrame(sample_history_data, history_schema)
    
    # Create temporary paths
    input_path = str(tmp_path / "input")
    output_path = str(tmp_path / "output")
    history_path = str(tmp_path / "history")
    
    # Write initial data
    input_df.write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(input_path)
        
    history_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(history_path)
    
    # Process the data with SCD
    success = process_actor_films(
        spark,
        input_path,
        output_path,
        history_path
    )
    assert success
    
    # Read processed data
    raw_df = spark.read.format("parquet").load(f"{output_path}/raw")
    stats_df = spark.read.format("parquet").load(f"{output_path}/actor_stats")
    history_df = spark.read.format("parquet").load(history_path)
    
    # Verify raw data
    assert raw_df.count() == 4  # Should have filtered out the invalid row
    assert len(raw_df.columns) == 8  # Including last_updated
    assert raw_df.filter("actor_id is null").count() == 0
    assert raw_df.filter("film_id is null").count() == 0
    
    # Verify stats
    assert stats_df.count() == 2  # Both actors have stats
    assert len(stats_df.columns) >= 8  # Including derived metrics
    
    # Verify SCD history
    current_records = history_df.filter("is_current = 'Y'").collect()
    assert len(current_records) == 2  # Both actors should have current records
    
    # Verify specific actor history
    tom_history = history_df.filter("actor_id = 'th123'").orderBy("valid_from").collect()
    assert len(tom_history) >= 2  # Should have at least old and new records
    
    # Verify SCD tracking
    old_records = history_df.filter("is_current = 'N'").collect()
    assert len(old_records) >= 2  # Should have old records marked as not current
    for record in old_records:
        assert record["valid_to"] is not None  # Old records should have end dates

def test_error_handling(spark, tmp_path):
    """Test comprehensive error handling"""
    input_path = str(tmp_path / "nonexistent")
    output_path = str(tmp_path / "output")
    history_path = str(tmp_path / "history")
    
    # Test with non-existent input path
    with pytest.raises(Exception):
        process_actor_films(spark, input_path, output_path, history_path)
    
    # Test with invalid schema
    input_path = str(tmp_path / "invalid")
    spark.createDataFrame([("invalid",)], ["single_column"]) \
        .write.mode("overwrite").format("csv").save(input_path)
    
    with pytest.raises(Exception):
        process_actor_films(spark, input_path, output_path, history_path)

def test_performance(spark, sample_data, tmp_path):
    """Test performance optimization settings and partitioning"""
    # Verify Spark configurations
    assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
    assert spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled") == "true"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "2"  # Test setting
    
    # Create a larger dataset for performance testing
    large_data = sample_data * 100  # Multiply data for testing
    schema = create_actor_films_schema()
    df = spark.createDataFrame(large_data, schema)
    
    input_path = str(tmp_path / "large_input")
    output_path = str(tmp_path / "large_output")
    history_path = str(tmp_path / "large_history")
    
    # Write test data
    df.write.mode("overwrite").format("csv").option("header", "true").save(input_path)
    
    # Process and verify it completes without error
    success = process_actor_films(spark, input_path, output_path, history_path)
    assert success
    
    # Verify output partitioning
    raw_files = spark.read.format("parquet").load(f"{output_path}/raw")
    stats_files = spark.read.format("parquet").load(f"{output_path}/actor_stats")
    
    # Verify the stats file has fewer partitions than the raw file
    assert raw_files.rdd.getNumPartitions() >= stats_files.rdd.getNumPartitions()
    
    # Verify partition sizes are reasonable
    raw_count = raw_files.count()
    stats_count = stats_files.count()
    assert raw_files.rdd.getNumPartitions() <= max(1, raw_count // 1000000 + 1)
    assert stats_files.rdd.getNumPartitions() <= max(1, stats_count // 100000 + 1) 