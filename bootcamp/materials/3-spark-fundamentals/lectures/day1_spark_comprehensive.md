# Apache Spark & Iceberg - Day 1 Comprehensive Guide

## 1. Introduction to Apache Spark

### What is Apache Spark?
Apache Spark is a unified analytics engine for large-scale data processing. It represents a significant evolution in big data processing:

- **Distributed Computing Framework**: 
  - Processes data in parallel across clusters
  - Handles both batch and streaming workloads
  - Supports multiple programming languages (Python, Scala, SQL, R)

- **Historical Evolution**:
  ```
  2009: Hadoop/MapReduce → 2015: Hive → 2017+: Spark
  ```
  - Hadoop/MapReduce: First-generation big data processing
  - Hive: SQL-like interface for Hadoop
  - Spark: Modern, in-memory processing engine

### Setting Up Your Environment

#### Prerequisites
- Docker Desktop installed and running
- Access to the Data Engineer Handbook repository
- Basic understanding of SQL and Python

#### Quick Start
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

## 2. Why Choose Spark?

### Key Advantages

1. **Efficient Memory Management**
   ```
   Traditional Processing:
   Disk → Process → Disk → Process → Disk
   
   Spark Processing:
   Disk → Memory → Process → Memory → Process → Disk
   ```
   - Keeps data in memory between operations
   - Only spills to disk when necessary
   - Intelligent memory management with Project Tungsten

2. **Storage Agnostic Architecture**
   ```python
   # Example: Reading from different sources
   
   # From CSV
   df_csv = spark.read.csv("path/to/file.csv")
   
   # From Database
   df_db = spark.read \
       .format("jdbc") \
       .option("url", "jdbc:postgresql://host:5432/db") \
       .option("dbtable", "schema.table") \
       .load()
   
   # From MongoDB
   df_mongo = spark.read \
       .format("mongo") \
       .option("uri", "mongodb://host/db.collection") \
       .load()
   ```

3. **Rich Ecosystem & Community Support**
   - Integration with ML libraries (MLlib)
   - Streaming capabilities (Structured Streaming)
   - Graph processing (GraphX)
   - Native SQL support (Spark SQL)

### When to Avoid Spark

1. **Team Knowledge Considerations**
   ```mermaid
   graph TD
   A[Team Assessment] --> B{Spark Knowledge?}
   B -->|Only You| C[Consider Alternatives]
   B -->|Multiple Team Members| D[Proceed with Spark]
   C --> E[Use Existing Stack]
   C --> F[Train Team]
   D --> G[Implement Spark]
   ```

2. **Technology Stack Alignment**
   - Example Scenario:
     ```
     Company Stack:
     - BigQuery for Data Warehouse
     - dbt for Transformations
     - Airflow for Orchestration
     
     Decision: Stick with BigQuery unless Spark offers significant advantages
     ```

## 3. Spark Architecture Deep Dive

### The Basketball Team Analogy

```ascii
+----------------+     +----------------+     +----------------+
|     PLAN       |     |    DRIVER     |     |   EXECUTORS   |
|  (The Play)    |     |  (The Coach)  |     | (The Players) |
+----------------+     +----------------+     +----------------+
| - SQL/Python   |     | - Optimizes   |     | - Process Data|
| - Lazy Eval    |     | - Coordinates |     | - Run Tasks   |
| - DAG Creation |     | - Schedules   |     | - Store Data  |
+----------------+     +----------------+     +----------------+
```

### 1. The Plan (The Play)

#### Code Examples
```python
# Example 1: Lazy Evaluation
df = spark.read.csv("data.csv")  # Nothing happens
df = df.filter(df.age > 25)      # Still nothing
result = df.count()              # Now it executes!

# Example 2: Action vs Transformation
# Transformation (Lazy)
filtered_df = df.filter(df.value > 100)
mapped_df = df.select(df.name, df.age * 2)

# Action (Triggers Execution)
count = df.count()
data = df.collect()
```

### 2. The Driver (The Coach)

#### Memory Configuration
```python
# Driver memory configuration
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.memoryOverhead", "1g") \
    .getOrCreate()
```

Key Settings Table:

| Setting | Default | Max | When to Increase |
|---------|---------|-----|-----------------|
| spark.driver.memory | 2GB | 16GB | Complex plans, large collects |
| spark.driver.memoryOverhead | 10% of memory | NA | JVM pressure |

### 3. The Executors (The Players)

#### Configuration Best Practices
```python
# Executor configuration
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memoryOverhead", "1g") \
    .getOrCreate()
```

Memory Sizing Strategy:
```python
def calculate_executor_memory(data_size_gb, num_executors):
    """
    Rule of thumb for executor memory sizing
    """
    base_memory = 2  # Base 2GB
    overhead_factor = 1.1  # 10% overhead
    
    per_executor = (data_size_gb / num_executors) * overhead_factor
    return max(base_memory, min(per_executor, 16))  # Cap at 16GB
```

## 4. Data Processing Deep Dive

### Join Operations Explained

#### 1. Shuffle Sort Merge Join
```python
# Example of shuffle sort merge join
large_df1 = spark.read.parquet("large_table1")
large_df2 = spark.read.parquet("large_table2")

# This will trigger shuffle sort merge
result = large_df1.join(large_df2, "key")
```

Visual Representation:
```
Table1 Partitions    Table2 Partitions    Result
[A,B,C]             [D,E,F]              [A,D]
[D,E,F]      →      [A,B,C]      →      [B,E]
[G,H,I]             [G,H,I]              [C,F]
```

#### 2. Broadcast Hash Join
```python
# Example of broadcast join
small_df = spark.read.parquet("small_table")
large_df = spark.read.parquet("large_table")

# Force broadcast
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")
```

#### 3. Bucket Join
```sql
-- Creating bucketed tables
CREATE TABLE bucketed_table1
USING parquet
CLUSTERED BY (key) INTO 8 BUCKETS
AS SELECT * FROM source_table1;

CREATE TABLE bucketed_table2
USING parquet
CLUSTERED BY (key) INTO 8 BUCKETS
AS SELECT * FROM source_table2;
```

### Advanced Shuffling Concepts

#### Shuffle Partitioning
```python
# Default shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 200)

# Dynamic shuffle partitions (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", True)
```

#### Handling Data Skew
```python
# Example: Handling skew in GROUP BY
from pyspark.sql.functions import rand

# Add random salt to spread data
df_with_salt = df.withColumn("salt", rand(seed=42))
           .groupBy("skewed_column", "salt")
           .agg(...)
           .groupBy("skewed_column")
           .agg(...)
```

## 5. Performance Optimization

### Memory Management Deep Dive

#### Driver Memory Management
```python
# Memory monitoring function
def monitor_driver_memory():
    import psutil
    process = psutil.Process()
    print(f"Memory Usage: {process.memory_info().rss / 1024 / 1024} MB")

# Use before/after large operations
monitor_driver_memory()
df.collect()  # Heavy operation
monitor_driver_memory()
```

#### Executor Memory Tuning
```python
# Progressive memory testing
def test_memory_configurations(df, memory_sizes=[2, 4, 6, 8]):
    for mem in memory_sizes:
        spark.conf.set("spark.executor.memory", f"{mem}g")
        start_time = time.time()
        try:
            df.cache().count()
            print(f"{mem}GB: Success - {time.time() - start_time}s")
        except Exception as e:
            print(f"{mem}GB: Failed - {str(e)}")
```

### File Organization and Optimization

#### Optimizing Sort Orders
```python
# Function to analyze column cardinality
def analyze_cardinality(df, columns):
    for col in columns:
        count = df.select(col).distinct().count()
        print(f"Column {col}: {count} distinct values")

# Use results to optimize sort order
df_optimized = df.sortWithinPartitions(
    "date",          # Lowest cardinality
    "category",      # Medium cardinality
    "user_id"        # Highest cardinality
)
```

## 6. Real-World Examples and Case Studies

### Netflix Case Study: 100TB/hour Processing
```python
# Original approach (problematic at scale)
def process_network_requests_v1(requests_df, ip_mapping_df):
    return requests_df.join(broadcast(ip_mapping_df), "ip")

# Improved approach
def process_network_requests_v2(requests_df):
    return requests_df.withColumn(
        "app_name",
        get_app_from_request_udf("request_headers")
    )
```

### Facebook Notifications Pipeline
```python
# Original implementation (expensive)
notifications_df.join(user_data_df, "user_id")

# Optimized implementation with bucketing
notifications_df.write \
    .bucketBy(1024, "user_id") \
    .saveAsTable("bucketed_notifications")

user_data_df.write \
    .bucketBy(1024, "user_id") \
    .saveAsTable("bucketed_user_data")
```

## 7. Troubleshooting Guide

### Common Issues and Solutions

#### 1. Out of Memory Errors
```python
# Problem:
df.collect()  # OOM!

# Solutions:
# 1. Use sampling
df.sample(0.1).collect()

# 2. Use windowing
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window = Window.orderBy("timestamp")
df.withColumn("row_num", row_number().over(window)) \
  .filter(col("row_num") <= 1000) \
  .drop("row_num") \
  .collect()
```

#### 2. Skewed Data Processing
```python
# Detect skew
def detect_skew(df, group_col):
    stats = df.groupBy(group_col).count() \
        .selectExpr(
            f"max(count)/avg(count) as skew_factor",
            "max(count) as max_count",
            "avg(count) as avg_count"
        ).collect()[0]
    
    print(f"Skew Factor: {stats.skew_factor}")
    print(f"Max Count: {stats.max_count}")
    print(f"Avg Count: {stats.avg_count}")
```

## 8. Best Practices Summary

### Code Organization
```
project/
├── src/
│   ├── jobs/
│   │   ├── __init__.py
│   │   └── process_data.py
│   ├── utils/
│   │   ├── __init__.py
│   │   └── spark_utils.py
│   └── tests/
│       ├── __init__.py
│       └── test_process_data.py
├── config/
│   └── spark_config.yaml
└── README.md
```

### Configuration Management
```python
# config/spark_config.yaml
development:
  driver_memory: "4g"
  executor_memory: "2g"
  shuffle_partitions: 200

production:
  driver_memory: "16g"
  executor_memory: "8g"
  shuffle_partitions: 1000

# src/utils/config_loader.py
def load_spark_config(env):
    import yaml
    with open("config/spark_config.yaml") as f:
        config = yaml.safe_load(f)
    return config[env]
```

## 9. Key Takeaways

1. **Architecture Understanding**
   - Know your driver, executors, and their roles
   - Understand memory management
   - Master shuffle operations

2. **Performance Optimization**
   - Start with proper configuration
   - Monitor and tune memory usage
   - Optimize join strategies
   - Handle data skew

3. **Development Best Practices**
   - Use version control
   - Implement proper testing
   - Follow code organization standards
   - Document configurations

4. **Production Readiness**
   - Monitor memory usage
   - Implement error handling
   - Use proper logging
   - Plan for scaling

Remember: Spark is powerful but requires careful consideration of your use case, team expertise, and existing technology stack. 