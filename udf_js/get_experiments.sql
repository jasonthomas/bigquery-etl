CREATE TEMP FUNCTION
  udf_js_get_experiments (input STRING)
  RETURNS ARRAY<STRUCT<
  id STRING,
  branchname STRING>>
  LANGUAGE js AS """
  const experiments = JSON.parse(input);
  return Object.entries(experiments).map(([id, data]) => ({ id, branchname: data.branch }));
  """;


-- Tests

SELECT
  assert_array_equals(udf_js_get_experiments('{"foo": {"branch": "a"}, "baz": {"branch": "b"}}'), [STRUCT("foo" AS id, "a" AS branchname), STRUCT("baz" AS id, "b" AS branchname)])
