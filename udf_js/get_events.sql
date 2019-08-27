CREATE TEMP FUNCTION
  udf_js_get_events(events_jsons ARRAY<STRUCT<process STRING, events_json STRING>>)
  RETURNS ARRAY<STRUCT<
    timestamp INT64,
    category STRING,
    method STRING,
    object STRING,
    string_value STRING,
    map_values ARRAY<STRUCT<key STRING, value STRING>>>>
  LANGUAGE js AS """
  return events_jsons.flatMap(({process, events_json}) =>
    JSON.parse(events_json).map((event) => {
      const pairs = Object.entries(event.map_values || {}).map(([key, value]) => ({key, value}));
      return {
        ...event,
        map_values: [{ key: "telemetry_process", value: process }, ...pairs]
      };
    }));
""";

-- Tests

WITH result AS (SELECT AS VALUE
  udf_js_get_events([('content', '[{"timestamp": 1, "category": "foo", "method": "GET", "map_values": {"b": "3"}}]'), ('dynamic', '[{"timestamp": 2, "category": "baz", "method": "GET", "string_value": "blee"}]'), ('gfx', '[{"timestamp": 3, "category": "boz", "method": "GET", "object": "{\\"z\\": \\"5\\"}", "string_value": "oif"}, {"timestamp": 4, "category": "biz", "method": "GET", "map_values": {"c": "7", "d": "8"}}]')]))
SELECT
  assert_equals(result[OFFSET(0)].timestamp, 1),
  assert_equals(result[OFFSET(0)].category, "foo"),
  assert_equals(result[OFFSET(0)].method, "GET"),
  assert_true(result[OFFSET(0)].object IS NULL),
  assert_true(result[OFFSET(0)].string_value IS NULL),
  assert_array_equals([STRUCT("telemetry_process", "content"), STRUCT("b", "3")], result[OFFSET(0)].map_values),

  assert_equals(result[OFFSET(1)].timestamp, 2),
  assert_equals(result[OFFSET(1)].category, "baz"),
  assert_equals(result[OFFSET(1)].method, "GET"),
  assert_null(result[OFFSET(1)].object),
  assert_equals(result[OFFSET(1)].string_value, "blee"),
  assert_array_equals([STRUCT("telemetry_process", "dynamic")], result[OFFSET(1)].map_values),

  assert_equals(result[OFFSET(2)].timestamp, 3),
  assert_equals(result[OFFSET(2)].category, "boz"),
  assert_equals(result[OFFSET(2)].method, "GET"),
  assert_equals(result[OFFSET(2)].object, '{"z": "5"}'),
  assert_equals(result[OFFSET(2)].string_value, "oif"),
  assert_array_equals([STRUCT("telemetry_process", "gfx")], result[OFFSET(2)].map_values),

  assert_equals(result[OFFSET(3)].timestamp, 4),
  assert_equals(result[OFFSET(3)].category, "biz"),
  assert_equals(result[OFFSET(3)].method, "GET"),
  assert_null(result[OFFSET(3)].object),
  assert_null(result[OFFSET(3)].string_value),
  assert_array_equals([STRUCT("telemetry_process", "gfx"), STRUCT("c", "7"), STRUCT("d", "8")], result[OFFSET(3)].map_values)

FROM
  result
