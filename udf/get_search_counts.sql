CREATE TEMP FUNCTION
  udf_get_search_counts(search_counts ARRAY<STRUCT<key STRING,
    value STRING>>) AS (
    ARRAY(
    SELECT
      AS STRUCT
      SUBSTR(key, 0, pos - 1) AS engine,
      SUBSTR(key, pos + 1) AS source,
      udf_json_extract_histogram(value).sum AS `count`
    FROM
      UNNEST(search_counts),
      UNNEST([REPLACE(key, "in-content.", "in-content:")]) AS key,
      UNNEST([STRPOS(key, ".")]) AS pos));

-- Tests

SELECT
  assert_array_equals(
    [STRUCT("this in-content:thing " AS `engine`, " that in-content:thing and other.period" AS `source`, 1 AS `count`)],
    udf_get_search_counts([("this in-content.thing . that in-content.thing and other.period", '{"sum":1}')]))
