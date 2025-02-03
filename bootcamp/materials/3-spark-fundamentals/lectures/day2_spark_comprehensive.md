# Apache Spark Advanced Concepts - Day 2 Comprehensive Guide

## 1. Development Environments and Deployment

### Spark Server vs. Notebooks

#### Spark Server (Traditional Approach)
```bash
# Example spark-submit command
spark-submit \
  --class com.company.MainClass \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 4g \
  path/to/application.jar
```

**Advantages:**
- Mirrors production environment exactly
- Version control through Git
- Better CI/CD integration
- More reliable for production workloads

**Disadvantages:**
- Slower development cycle
- JAR upload time can be significant
- More complex setup required

#### Notebook Environment (Databricks Style)
```python
# Example notebook session
spark = SparkSession.builder \
    .appName("NotebookJob") \
    .getOrCreate()
```

**Advantages:**
- Rapid development and iteration
- Interactive debugging
- Great for exploration and POCs
- Lower barrier to entry

**Disadvantages:**
- Risk of unchecked mutations in production
- No native version control
- Can mask performance issues
- State management challenges

### Production Considerations

#### Version Control Best Practices
```
project/
├── notebooks/
│   └── exploration/
├── src/
│   ├── main/
│   │   └── scala/
│   └── test/
│       └── scala/
├── build.sbt
└── README.md
```

#### CI/CD Pipeline Example
```yaml
# Example GitHub Actions workflow
name: Spark Pipeline
on: [push]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run tests
        run: sbt test
      - name: Package application
        run: sbt package
```

## 2. API Choices and Their Use Cases

### 1. Spark SQL
```sql
-- Best for data scientists and analysts
CREATE TEMPORARY VIEW user_metrics AS
SELECT 
    user_id,
    COUNT(*) as event_count,
    COUNT(DISTINCT device_id) as device_count
FROM events
GROUP BY user_id;
```

**When to Use:**
- Quick analysis and exploration
- Collaboration with SQL-proficient team members
- Simple transformations
- Ad-hoc queries

### 2. DataFrame API
```python
# Best for data engineers (Python)
from pyspark.sql.functions import count, countDistinct

user_metrics = events.groupBy("user_id") \
    .agg(
        count("*").alias("event_count"),
        countDistinct("device_id").alias("device_count")
    )
```

**When to Use:**
- Complex transformations
- Need for modularity
- Python-based teams
- Balance of performance and usability

### 3. Dataset API (Scala)
```scala
// Best for type safety and performance
case class Event(
    userId: String,
    deviceId: Option[String],
    timestamp: Long
)

val events: Dataset[Event] = spark.read
    .parquet("events")
    .as[Event]
```

**When to Use:**
- Strong type safety requirements
- Complex business logic
- Performance-critical applications
- Scala-based teams

## 3. Advanced Performance Optimization

### Caching Strategies

#### Memory-Only Caching
```scala
// Default caching strategy
df.cache()  // Same as persist(StorageLevel.MEMORY_ONLY)

// Explicit memory-only persistence
import org.apache.spark.storage.StorageLevel
df.persist(StorageLevel.MEMORY_ONLY)
```

#### Memory Management
```scala
// Monitor cache usage
def monitorCacheUsage(df: DataFrame): Unit = {
    println(s"Is cached: ${df.storageLevel != StorageLevel.NONE}")
    println(s"Memory used: ${df.queryExecution.optimizedPlan.stats.sizeInBytes / 1024 / 1024} MB")
}
```

### Bucket Join Optimization

#### Creating Bucketed Tables
```sql
-- Example of bucketing in Iceberg
CREATE TABLE events_bucketed
USING iceberg
CLUSTERED BY (user_id) INTO 16 BUCKETS
PARTITIONED BY (date)
AS SELECT * FROM events;
```

#### Bucket Join Performance Analysis
```scala
// Disable broadcast joins to force bucket join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

// Compare execution plans
def comparePlans(df1: DataFrame, df2: DataFrame): Unit = {
    println("=== Non-bucketed Join ===")
    df1.explain(true)
    println("=== Bucketed Join ===")
    df2.explain(true)
}
```

### UDF Performance

#### Python UDF (Less Performant)
```python
# Python UDF with serialization overhead
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def clean_text_py(text):
    return text.strip().lower()
```

#### Scala UDF (More Performant)
```scala
// Scala UDF with native performance
val cleanTextScala = udf((text: String) => text.trim.toLowerCase)
```

## 4. Advanced Data Modeling

### Schema Management with Case Classes
```scala
// Type-safe schema definition
case class UserEvent(
    userId: String,
    deviceId: Option[String],
    eventType: String,
    timestamp: Long,
    properties: Map[String, String]
)

// Schema evolution handling
case class UserEventV2(
    userId: String,
    deviceId: Option[String],
    eventType: String,
    timestamp: Long,
    properties: Map[String, String],
    version: Int = 2
)
```

### Complex Transformations
```scala
// Example of complex business logic
def processEvents(events: Dataset[UserEvent]): Dataset[UserMetrics] = {
    events
        .filter(_.userId.nonEmpty)
        .groupByKey(_.userId)
        .mapGroups { case (userId, events) =>
            val sortedEvents = events.toSeq.sortBy(_.timestamp)
            UserMetrics(
                userId = userId,
                firstSeen = sortedEvents.head.timestamp,
                lastSeen = sortedEvents.last.timestamp,
                totalEvents = sortedEvents.size
            )
        }
}
```

## 5. Testing and Quality Assurance

### Unit Testing
```scala
// Example test case using ScalaTest
class EventProcessingSpec extends AnyFlatSpec with Matchers {
    "processEvents" should "calculate correct metrics" in {
        val testEvents = Seq(
            UserEvent("user1", Some("device1"), "click", 1000L, Map()),
            UserEvent("user1", Some("device2"), "view", 2000L, Map())
        )
        
        val result = processEvents(testEvents.toDS())
        
        result.collect() should contain(
            UserMetrics("user1", 1000L, 2000L, 2)
        )
    }
}
```

### Integration Testing
```scala
// Integration test with actual Spark session
class SparkIntegrationSpec extends AnyFlatSpec with SparkSessionTestWrapper {
    "Bucketed join" should "be more efficient than regular join" in {
        // Setup test data
        val events = createTestEvents()
        val users = createTestUsers()
        
        // Compare execution plans
        val regularJoin = events.join(users, "userId")
        val bucketedJoin = events_bucketed.join(users_bucketed, "userId")
        
        // Assert no shuffle in bucketed join plan
        bucketedJoin.queryExecution.executedPlan.toString should not include "Exchange"
    }
}
```

## 6. Best Practices Summary

1. **Development Environment**
   - Use notebooks for exploration
   - Use proper version control for production code
   - Implement CI/CD pipelines

2. **API Selection**
   - Spark SQL: Quick analysis and collaboration
   - DataFrame API: General purpose transformations
   - Dataset API: Type-safe, performance-critical applications

3. **Performance Optimization**
   - Use appropriate caching strategy
   - Implement bucket joins for large-scale operations
   - Choose UDFs carefully based on language and performance needs

4. **Testing**
   - Implement comprehensive unit tests
   - Use integration tests for complex operations
   - Test with realistic data volumes

5. **Production Deployment**
   - Use proper version control
   - Implement monitoring and alerting
   - Plan for data quality checks

Remember: The choice between different Spark approaches should be based on your specific use case, team expertise, and performance requirements. 