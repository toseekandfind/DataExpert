# Apache Spark & Iceberg Fundamentals - Day 1 Notes

## 1. Introduction to Apache Spark

### What is Spark?
- Distributed compute framework for processing large amounts of data efficiently
- Successor to earlier big data technologies (Hadoop/MapReduce → Hive → Spark)
- Currently dominant in the big data processing landscape

### Historical Context
- 2009: Hadoop and Java MapReduce
- 2015-2017: Hive era
- 2017-Present: Spark dominance

## 2. Why Choose Spark?

### Key Advantages
1. **Efficient RAM Usage**
   - Better memory utilization compared to predecessors
   - Minimizes disk writes/reads
   - Only spills to disk when memory is insufficient

2. **Storage Agnostic**
   - Can read from multiple sources:
     - Relational databases
     - Data lakes
     - Files
     - MongoDB
     - And more

3. **Strong Community Support**
   - Large ecosystem
   - Platform independent (not tied to Databricks)

### When Not to Use Spark
1. **Team Knowledge Gap**
   - If you're the only one who knows Spark
   - Better to train team or use existing technology

2. **Existing Technology Stack**
   - If company heavily uses alternatives (BigQuery, Snowflake)
   - Maintain homogeneity in pipeline technologies

## 3. Spark Architecture

### Core Components (Basketball Team Analogy)
1. **The Plan** (The Play)
   - Written in Python, Scala, SQL, or R
   - Evaluated lazily
   - Executes on action (write/collect)

2. **The Driver** (The Coach)
   - Reads and optimizes the plan
   - Key settings:
     - `spark.driver.memory` (default 2GB, max 16GB)
     - Memory overhead for JVM

3. **The Executors** (The Players)
   - Perform actual data processing
   - Settings:
     - `spark.executor.memory`
     - `spark.executor.cores` (recommended: 4-6)
     - Memory overhead

## 4. Data Processing Concepts

### Join Types
1. **Shuffle Sort Merge Join**
   - Least performant but most versatile
   - Works in all scenarios

2. **Broadcast Hash Join**
   - One side must be small (< 8-10GB)
   - More efficient than shuffle

3. **Bucket Join**
   - Requires pre-bucketed data
   - Very efficient for multiple joins

### Shuffling
- Least scalable part of Spark
- Becomes problematic at 20-30TB scale
- Example: Network request processing at Netflix (100TB/hour)

### Handling Data Skew
1. **Modern Solution**
   - Use `spark.sql.adaptive.enabled=true` (Spark 3+)

2. **Legacy Solutions**
   - Random number technique for GROUP BY
   - Filter out outliers
   - Separate pipeline for outliers

## 5. Best Practices

### Development Environment
1. **Notebooks**
   - Good for proof of concept
   - Less ideal for production
   - Popular in Databricks

2. **Production Code**
   - Use git repository
   - Implement spark-submit
   - Include unit tests

### Data Management
1. **Partitioning**
   - Partition by date for most datasets
   - Use execution date from pipeline

2. **File Organization**
   - Sort data within partitions
   - Consider cardinality when sorting
   - Use run-length encoding for compression

### Performance Optimization
1. **Memory Management**
   - Monitor driver and executor memory
   - Use `collect` carefully
   - Filter/aggregate before collecting

2. **Sorting Strategy**
   - Prefer `sortWithinPartitions` over `sort`
   - Avoid global sorts for large datasets

3. **Monitoring**
   - Use `explain` to understand query plans
   - Watch for exchange/shuffle operations

## 6. Key Takeaways
- Spark is powerful for large-scale data processing but requires careful configuration
- Memory management and shuffling are critical performance factors
- Understanding join types and their appropriate use cases is essential
- Always consider team expertise and existing technology stack when choosing Spark
- Production deployment requires proper engineering practices beyond notebook development 