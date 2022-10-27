# Riffl
![gradle](https://github.com/riffl/riffl/actions/workflows/gradle.yml/badge.svg)

Riffl is a generic streaming data ingestion framework currently executing on top of Flink and leveraging Table API.
It aims for its processing to be simple to define and reason about with YAML configuration and SQL expressions.
Deploys into a wide range of environments be it Hadoop, Kubernetes, or in any other Flink supported ways, and it is self-contained.
Riffl puts data quality first with exactly-once guarantees but also output optimization so that query engines can utilize their features to operate efficiently.

#### Features
* Streaming directly into storage ready for querying
* High performance, horizontally scalable, low latency
* Exactly-once guarantees
* No coding required only Yaml and SQL
* Output optimization

# Configuration

#### config.yaml
Distributed file system location (hdfs|s3a|file|...) hdfs:///riffl/config/config.yaml
```
name: Riffl application
# [optional] Properties to expand placeholders specified as ${property...}
properties:
  catalog.name: "custom_catalog"
  
# Execution configuration overrides
execution:
  type: FLINK
  configuration:
    execution.checkpointing.interval: 45s
    execution.checkpointing.mode: EXACTLY_ONCE

# [optional] Catalog/Database definitions to support external integration points like Hive or Iceberg
catalogs:                                        
  - createUri: hdfs:///riffl/example/catalog.ddl
   [create: "CREATE CATALOG ${properties.catalog.name} (...)"]             
databases:                                          
  - createUri: example/database.ddl
   [create: "CREATE DATABASE (...)"]
   
# Source definitions to load data from e.g. Kafka, Kinesis
sources:
  - createUri: example/source.ddl
   [create: "CREATE TABLE (...)"]
    mapUri: example/source-map.ddl
   [map: "SELECT column FROM (...)"]
    # Source stream rebalance in case of input data skew  [default: false]
    rebalance: false

# Sink definitions to define output location and format e.g. AWS S3 as Parquet
sinks:
  - createUri: example/sink-1.ddl
   [create: "CREATE TABLE (...)"]
   # Name of a table if already created in an external catalog
   [table: "iceberg_catalog.riffle.sink_1"]
    queryUri: example/sink-1-query.ddl
   [query: "SELECT column FROM (...)"]
  - createUri: example/sink-2.ddl
    queryUri: example/sink-2-query.ddl
    
    # [optional] Parallism of sink [dafault: application paralleism]
    parallelism: 5
    # [optional] Custom data distribution configuartion to optimize the output  
    distribution:
      className: "io.riffl.sink.row.KeyByTaskAssigner"
      properties:
        keys:
          - "someField_2"
        keyParallelism: 2
```
#### Source

Data source and format defined as one of [Flink connectors](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/table/overview/) supporting the "Unbounded Scan". 

```
e.g. location hdfs:///riffl/config/source.ddl
CREATE TABLE source_table (
    `timestamp` STRING,
    `user` STRING,
    product STRING,
    price DOUBLE,
    ingredients ARRAY<STRING>
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'test-topic',
    'connector.properties.bootstrap.servers' = '<ips>',
    'format.type' = 'json',
    'format.fail-on-missing-field' = 'false'
)
```

#### Sink

Data output destination and format defined with standard [filesystem connector](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/table/filesystem/) or custom e.g. [Iceberg](https://iceberg.apache.org/docs/latest/flink-connector/) connector. 

```
e.g. location hdfs:///riffl/config/sink.ddl
CREATE TABLE IF NOT EXISTS sink_default (
  product_id INT,
  product_type INT,
  product_name STRING,
  product_desc STRING,
  dt STRING,
  hr STRING
) PARTITIONED BY (dt, hr)
WITH (
'connector'='filesystem',
'format'='parquet',
'path'='${properties.sink.path}'
)"
```
# Deployment
Supported Flink versions:
* 1.14

### Build
```
./gradlew clean build
```

### Local
```
./gradlew runLocal --args='--application example/application.yaml'
```
