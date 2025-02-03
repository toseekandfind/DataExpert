# Apache Spark & Iceberg Lab - Day 1 Notes

## 1. Lab Setup and Environment

### Prerequisites
- Docker Desktop installed and running
- Access to the Data Engineer Handbook repository
- Basic understanding of SQL and Python

### Environment Setup
```bash
# Two options for starting the environment:
make up
# OR
docker-compose up
```

The setup includes four essential containers:
1. Iceberg container
2. MinIO container
3. MC container
4. Spark Iceberg container

Access point: `localhost:8888`

## 2. Understanding PySpark DataFrames

### Basic DataFrame Operations

#### Creating a Spark Session
```python
# Building a Spark session
spark = SparkSession.builder \
    .appName("Jupiter") \
    .getOrCreate()
```

**Important Note**: The syntax follows Java/Scala conventions rather than Python conventions due to PySpark being a JVM wrapper:
- Uses camelCase instead of snake_case
- Example: `.getOrCreate()` instead of `get_or_create()`

### Memory Management Examples

#### Dangerous Operations Example
```python
# This can cause Out of Memory errors
df.collect()  # Brings entire dataset to driver

# Better approach
df.take(5)    # Limits data returned to driver
df.show()     # Shows only a sample
```

#### Cross Join Example (Memory Issues Demonstration)
```python
# Creating a problematic cross join
problem_df = df.crossJoin(df.select(lit(1).alias('dummy')))
problem_df.collect()  # Will likely cause OOM error

# Safe approach
problem_df.take(5)   # Returns only 5 rows safely
```

## 3. Data Partitioning and Sorting

### Partitioning Strategies

#### Basic Partitioning
```python
# Repartitioning data into 10 partitions by event_date
df = df.repartition(10, "event_date")
```

### Sorting Strategies

#### Global Sort vs. Sort Within Partitions

1. **Global Sort (Not Recommended for Large Datasets)**
```python
# Global sort - causes additional shuffle
df_sorted = df.sort("event_date", "host", "browser_family")
```

2. **Sort Within Partitions (Recommended)**
```python
# More efficient - sorts within existing partitions
df_sorted = df.sortWithinPartitions("event_date", "host", "browser_family")
```

### Understanding Query Plans

#### Analyzing Sort Operations
```python
# Examining query plan for global sort
df_sorted.explain()

# Key differences in query plans:
# 1. Global sort shows additional 'exchange' (shuffle) operation
# 2. Sort within partitions avoids extra shuffle
```

Example Query Plan Output:
```
== Physical Plan ==
*(1) FileScan csv
+- *(2) Project
+- *(3) Exchange hashpartitioning
+- *(4) Sort [event_date, host, browser_family]
```

## 4. Working with Apache Iceberg

### Table Creation and Management

#### Creating Iceberg Tables
```sql
CREATE TABLE IF NOT EXISTS bootcamp.events_sorted
USING iceberg
PARTITIONED BY (years(event_date))
AS SELECT * FROM sorted_df
```

#### Partition Specifications
- Can partition by:
  - Years: `years(timestamp_column)`
  - Months: `months(timestamp_column)`
  - Days: `days(timestamp_column)`
  - Hours: `hours(timestamp_column)`
  - Custom columns: `column_name`

### File Organization and Optimization

#### Analyzing File Sizes
```sql
SELECT 
    sum(file_size_in_bytes) as size,
    count(1) as num_files
FROM bootcamp.events_sorted.files
UNION ALL
SELECT 
    sum(file_size_in_bytes) as size,
    count(1) as num_files
FROM bootcamp.events_unsorted.files;
```

#### Optimizing File Sizes Through Sorting

1. **Single Column Sort**
```python
# Sorting by just event_date
df_date_sorted = df.sortWithinPartitions("event_date")
```

2. **Multiple Column Sort (Better Compression)**
```python
# Sorting by multiple columns in cardinality order
df_optimized = df.sortWithinPartitions(
    "event_date",      # Lowest cardinality
    "browser_family",  # Medium cardinality
    "host"            # Highest cardinality
)
```

### Run Length Encoding Benefits
- Sorting data by low cardinality columns first improves compression
- Can achieve 10-15% reduction in file size
- Example compression improvements:
  - No sorting: 2.9MB
  - Date-only sort: 2.85MB
  - Full sort (date, browser, host): 2.5MB

## 5. Performance Tips and Tricks

### Memory Management Best Practices

1. **Driver Memory**
   - Default: 2GB
   - Max: 16GB
   - When to increase:
     - Complex job with many steps
     - Large collect operations
     - Heavy UDF usage

2. **Executor Memory**
   - Start with lower values (2-4GB)
   - Test with increments (4GB, 6GB, 8GB)
   - Find minimum stable value
   - Avoid default max (16GB) without testing

### Executor Cores Configuration
- Default: 4 cores per executor
- Maximum recommended: 6 cores
- Why not more?
  - Disk I/O becomes bottleneck
  - Increased risk of OOM with more concurrent tasks
  - Higher probability of skewed tasks affecting performance

### Memory Overhead Considerations
- Separate from main memory allocation
- Important for:
  - JVM operations
  - UDF execution
  - Complex operations (many joins/unions)
- Can increase without increasing main memory
- Good first step for stabilizing unreliable jobs

## 6. Lab Exercises and Common Issues

### Troubleshooting Examples

1. **OOM in Driver**
```python
# Problem:
large_df.collect()

# Solution:
large_df.take(n)
# or
large_df.show()
```

2. **Partition Issues**
```python
# Check partition distribution
df.groupBy(spark_partition_id()).count().show()
```

3. **Query Plan Analysis**
```python
# Detailed query plan
df.explain(True)

# Basic query plan
df.explain()
```

### Best Practices Demonstrated
1. Always check query plans before execution
2. Monitor memory usage
3. Use appropriate sorting strategies
4. Optimize file sizes through proper column ordering
5. Test with smaller datasets first

## 7. Key Lab Learnings

1. **Practical Implementation**
   - Setting up local Spark environment
   - Working with real datasets
   - Understanding memory limitations

2. **Performance Optimization**
   - Proper sorting strategies
   - Efficient partitioning
   - File size optimization

3. **Troubleshooting Skills**
   - Reading query plans
   - Identifying memory issues
   - Solving common problems

4. **Best Practices**
   - When to use different operations
   - How to optimize for performance
   - Real-world considerations 