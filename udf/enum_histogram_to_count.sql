/*

Find the largest numeric bucket that contains a value greater than zero.

https://github.com/mozilla/telemetry-batch-view/blob/ea0733c/src/main/scala/com/mozilla/telemetry/utils/MainPing.scala#L253-L266

*/

CREATE TEMP FUNCTION
  udf_enum_histogram_to_count(histogram STRING) AS ((
    SELECT
      MAX(value)
    FROM
      UNNEST(udf_json_extract_histogram(histogram).values)
    WHERE
      value > 0));

-- Tests

SELECT
  assert_equals(2, udf_enum_histogram_to_count('{"values":{"0":1,"1":2}}')),
  assert_null(udf_enum_histogram_to_count('{}')),
  assert_null(udf_enum_histogram_to_count('{"values":{"0":0}}')),
  assert_equals(1, udf_enum_histogram_to_count('{"values":{"5":1}}'))
