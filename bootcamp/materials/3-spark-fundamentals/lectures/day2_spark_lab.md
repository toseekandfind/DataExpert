# Apache Spark Advanced Lab - Day 2

## Lab Setup and Environment

### Prerequisites
- Docker Desktop installed and running
- Access to the Data Engineer Handbook repository
- Basic understanding of SQL and Python/Scala

### Environment Setup
```bash
# Option 1: Using make
make up

# Option 2: Using docker-compose
docker-compose up
```

The environment includes:
1. Iceberg container (table format)
2. MinIO container (object storage)
3. MC container (MinIO client)
4. Spark Iceberg container (processing engine)

Access your Spark environment at: `localhost:8888`

## Lab 1: API Comparison

### 1. Dataset API Example (Scala)
```scala
// Define schema using case classes
case class Event(
    userId: String,
    deviceId: Option[String],
    referrer: String,
    hostUrl: String,
    url: String,
    eventTime: String
)

case class Device(
    deviceId: String,
    browserType: String,
    osType: String,
    deviceType: String
)

// Read and transform data
val events: Dataset[Event] = spark.read
    .option("header", true)
    .csv("events.csv")
    .as[Event]
    .filter(event => event.userId.isDefined && event.deviceId.isDefined)

val devices: Dataset[Device] = spark.read
    .option("header", true)
    .csv("devices.csv")
    .as[Device]
```

### 2. DataFrame API Example
```scala
// Same operations using DataFrame API
val eventsDF = spark.read
    .option("header", true)
    .csv("events.csv")
    .filter($"user_id".isNotNull && $"device_id".isNotNull)

val devicesDF = spark.read
    .option("header", true)
    .csv("devices.csv")
```

### 3. Spark SQL Example
```sql
-- Same operations using SQL
CREATE TEMPORARY VIEW events_view AS
SELECT *
FROM events
WHERE user_id IS NOT NULL 
AND device_id IS NOT NULL;

CREATE TEMPORARY VIEW devices_view AS
SELECT *
FROM devices;
```

## Lab 2: Caching Deep Dive

### 1. Memory-Only Caching
```scala
// Import required for storage levels
import org.apache.spark.storage.StorageLevel

// Example dataset
val events = spark.read.parquet("events")

// Cache the dataset
events.cache()  // or events.persist(StorageLevel.MEMORY_ONLY)

// First action will cache
events.count()

// Subsequent actions use cache
events.select("user_id").distinct().count()

// Clean up
events.unpersist()
```

### 2. Comparing Execution Plans
```scala
// Function to analyze execution plans
def analyzePlans(df: DataFrame): Unit = {
    println("=== Without Cache ===")
    df.explain(true)
    
    df.cache()
    df.count()  // Materialize cache
    
    println("=== With Cache ===")
    df.explain(true)
}
```

## Lab 3: Bucket Join Implementation

### 1. Create Bucketed Tables
```sql
-- Create bucketed tables in Iceberg
CREATE TABLE matches_bucketed
USING iceberg
PARTITIONED BY (date)
CLUSTERED BY (match_id) INTO 16 BUCKETS
AS SELECT * FROM matches;

CREATE TABLE match_details_bucketed
USING iceberg
CLUSTERED BY (match_id) INTO 16 BUCKETS
AS SELECT * FROM match_details;
```

### 2. Compare Join Performance
```scala
// Disable broadcast joins to force bucket join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

// Non-bucketed join
val regularJoin = spark.sql("""
    SELECT m.*, d.player_id, d.position
    FROM matches m
    JOIN match_details d ON m.match_id = d.match_id
""")

// Bucketed join
val bucketedJoin = spark.sql("""
    SELECT m.*, d.player_id, d.position
    FROM matches_bucketed m
    JOIN match_details_bucketed d ON m.match_id = d.match_id
""")

// Compare execution plans
regularJoin.explain(true)
bucketedJoin.explain(true)
```

## Lab 4: UDF Performance Testing

### 1. Python UDF Example
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define Python UDF
@udf(returnType=StringType())
def clean_text_py(text):
    return text.strip().lower()

# Apply Python UDF
df_py = events.withColumn("clean_text", clean_text_py("text"))
```

### 2. Scala UDF Example
```scala
// Define Scala UDF
val cleanTextScala = udf((text: String) => text.trim.toLowerCase)

// Apply Scala UDF
val dfScala = events.withColumn("clean_text", cleanTextScala($"text"))
```

### 3. Performance Comparison
```scala
def compareUDFPerformance(): Unit = {
    // Generate test data
    val testData = (1 to 1000000).map(i => (s"TEXT$i  ")).toDF("text")
    
    // Time Python UDF
    val startPy = System.nanoTime()
    val resultPy = testData.withColumn("clean", clean_text_py($"text"))
    resultPy.count()
    val timePy = (System.nanoTime() - startPy) / 1e9d
    
    // Time Scala UDF
    val startScala = System.nanoTime()
    val resultScala = testData.withColumn("clean", cleanTextScala($"text"))
    resultScala.count()
    val timeScala = (System.nanoTime() - startScala) / 1e9d
    
    println(s"Python UDF time: $timePy seconds")
    println(s"Scala UDF time: $timeScala seconds")
}
```

## Lab 5: Testing Implementation

### 1. Unit Testing Setup
```scala
import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession

class SparkTransformationsTest extends FunSuite {
    lazy val spark = SparkSession.builder()
        .master("local[*]")
        .appName("unit-testing")
        .getOrCreate()
        
    import spark.implicits._
    
    test("clean_text UDF should lowercase and trim text") {
        // Create test data
        val testDF = Seq(
            "  HELLO  ",
            "World  ",
            "  Test"
        ).toDF("text")
        
        // Apply transformation
        val resultDF = testDF.withColumn("clean", cleanTextScala($"text"))
        
        // Verify results
        val results = resultDF.collect()
        assert(results(0).getString(1) === "hello")
        assert(results(1).getString(1) === "world")
        assert(results(2).getString(1) === "test")
    }
}
```

### 2. Integration Testing
```scala
class SparkIntegrationTest extends FunSuite {
    test("bucket join should be more efficient than regular join") {
        // Setup test data
        val matches = createTestMatches()
        val details = createTestDetails()
        
        // Perform joins
        val regularJoin = matches.join(details, "match_id")
        val bucketedJoin = matches_bucketed.join(details_bucketed, "match_id")
        
        // Compare execution plans
        val regularPlan = regularJoin.queryExecution.executedPlan.toString
        val bucketedPlan = bucketedJoin.queryExecution.executedPlan.toString
        
        // Verify bucketed join doesn't include shuffle
        assert(!bucketedPlan.contains("Exchange"))
    }
}
```

## Common Issues and Troubleshooting

1. **Memory Issues**
   - Symptom: OutOfMemoryError
   - Solution: Adjust executor memory or partition data appropriately
   ```scala
   spark.conf.set("spark.executor.memory", "4g")
   ```

2. **Bucket Join Not Working**
   - Symptom: Still seeing shuffle in execution plan
   - Solution: Verify bucketing configuration and disable broadcast joins
   ```scala
   spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
   ```

3. **Caching Problems**
   - Symptom: Unexpected recomputation
   - Solution: Verify cache materialization and storage level
   ```scala
   // Force cache materialization
   df.cache()
   df.count()
   ```

## Performance Tips

1. **Memory Management**
   - Monitor executor memory usage
   - Use appropriate storage levels for caching
   - Clean up unnecessary caches with unpersist()

2. **Join Optimization**
   - Use bucket joins for large datasets
   - Enable adaptive query execution for skewed data
   - Consider broadcast joins for small tables

3. **UDF Efficiency**
   - Use Scala UDFs for better performance
   - Minimize serialization overhead
   - Consider built-in functions when possible

## Key Lab Learnings

1. **API Selection**
   - Dataset API provides compile-time type safety
   - DataFrame API offers good balance of usability and performance
   - Spark SQL is great for quick analysis

2. **Performance Optimization**
   - Caching is beneficial only for reused datasets
   - Bucket joins can significantly reduce shuffle
   - UDF implementation language affects performance

3. **Testing Practices**
   - Unit tests for individual transformations
   - Integration tests for end-to-end workflows
   - Performance testing for optimization verification 