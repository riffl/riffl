SELECT
  someField_0,
  someField_1,
  MOD(HASH_CODE(someField_0), 3) as someField_2
FROM source_2
WHERE MOD(HASH_CODE(someField_0), 3) = 1
