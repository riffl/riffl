CREATE TABLE IF NOT EXISTS source_2 (
    someField_0 STRING,
    someField_1 STRING,
    someField_2 STRING,
    someField_3 STRING,
    someField_4 STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '100'
)
