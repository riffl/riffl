# Riffl

Riffl is a generic streaming data ingestion framework currently build on top of Flink leveraging Table API.
It aims for its processing to be simple to define and reason about with YAML configuration and SQL expressions.
Deploys into wide range of environments be it Hadoop, Kubernetes or in any other Flink supported ways, and it is self-contained.
Riffl puts data quality first with exactly-once guarantees but also output optimization so that query engines can utilise their features to operate efficiently.

#### Features
* Streaming directly into storage ready for querying
* High performance, horizontally scalable, low latency
* Exactly-once guarantees
* No coding required only Yaml+SQL
* Output optimized for querying

# Configuration

#### config.yaml
```
e.g. location (hdfs|s3a|file|...) hdfs:///riffl/config/config.yaml
name: Riffl application
type: ddl

# Flink execution configuration overrides
configurationOverrides:
  execution.checkpointing.interval: 45s
  execution.checkpointing.mode: EXACTLY_ONCE

# [Optional] Catalog/Database definitions to support external integration points like Hive or Iceberg
catalogs:                                        
  - createUri: example/catalog.ddl                
databases:                                          
  - createUri: example/database.ddl
  
# Source definitions to load data from e.g. Kafka, Kinesis
sources:
  - createUri: example/source-1.ddl
    mapUri: example/source-1-map.ddl
    rebalance: false

# Sink definitions to define output location and format e.g. AWS S3 as Parquet
sinks:
  - createUri: example/sink-1.ddl
    queryUri: example/sink-1-query.ddl
  - createUri: example/sink-2.ddl
    queryUri: example/sink-2-query.ddl
    # Custom data distribution configuartion to optimise the output  
    distribution:
      className: "io.riffl.sink.distribution.KeyedTaskAssigner"
      parallelism: 5
      properties:
        keys:
          - "someField_2"
        keyParallelism: 2
```
#### source.ddl

Source must support "Unbounded Scan" - [Flink connectors](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/table/overview/).

```
e.g. location hdfs:///riffl/config/source.ddl
CREATE TABLE source_table_1 (
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
