import pytest
from pyspark.sql import SparkSession
from jobs.actor_films_job import create_actor_films_schema, process_actor_films

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing"""
    return SparkSession.builder \
        .appName("TestActorFilmsJob") \
        .master("local[1]") \
        .getOrCreate()

def test_create_actor_films_schema(spark):
    """Test schema creation"""
    schema = create_actor_films_schema()
    
    # Create a test DataFrame with the schema
    test_data = [
        ("Tom Hanks", "th123", "Forrest Gump", 1994, 1000000, 8.8, "fg456"),
        ("Morgan Freeman", "mf789", "Shawshank Redemption", 1994, 2000000, 9.3, "sr789")
    ]
    
    df = spark.createDataFrame(test_data, schema)
    
    # Verify schema types
    assert df.schema["actor"].dataType.simpleString() == "string"
    assert df.schema["actor_id"].dataType.simpleString() == "string"
    assert df.schema["film"].dataType.simpleString() == "string"
    assert df.schema["year"].dataType.simpleString() == "int"
    assert df.schema["votes"].dataType.simpleString() == "int"
    assert df.schema["rating"].dataType.simpleString() == "float"
    assert df.schema["film_id"].dataType.simpleString() == "string"

def test_process_actor_films(spark, tmp_path):
    """Test the processing logic"""
    # Create test input data
    test_data = [
        ("Tom Hanks", "th123", "Forrest Gump", 1994, 1000000, 8.8, "fg456"),
        ("Morgan Freeman", "mf789", "Shawshank Redemption", 1994, 2000000, 9.3, "sr789"),
        (None, None, "Invalid Movie", 2000, 100, 5.0, None)  # This row should be filtered out
    ]
    
    input_df = spark.createDataFrame(test_data, create_actor_films_schema())
    
    # Create temporary paths for testing
    input_path = str(tmp_path / "input")
    output_path = str(tmp_path / "output")
    
    # Write test data
    input_df.write.mode("overwrite").format("csv").option("header", "true").save(input_path)
    
    # Process the data
    process_actor_films(spark, input_path, output_path)
    
    # Read processed data
    result_df = spark.read.format("parquet").load(output_path)
    
    # Verify results
    assert result_df.count() == 2  # Should have filtered out the invalid row
    assert len(result_df.columns) == 7
    
    # Verify data quality
    assert result_df.filter("actor_id is null").count() == 0
    assert result_df.filter("film_id is null").count() == 0 