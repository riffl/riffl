CREATE TABLE IF NOT EXISTS sink_1 (
  someField_0 STRING,
  someField_1 STRING,
  someField_2 INT
) PARTITIONED BY (someField_2)
WITH (
  'connector'='filesystem',
  'format'='csv',
  'path'='file:///tmp/riffl'
)
