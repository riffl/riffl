CREATE TABLE IF NOT EXISTS `custom_catalog`.`default`.`sink_2` (
  someField_0 STRING,
  someField_1 STRING,
  someField_2 INT
) PARTITIONED BY (someField_2)
WITH (
  'connector'='filesystem',
  'format'='parquet',
  'path'='${overrides.sink-2.path}'
)
