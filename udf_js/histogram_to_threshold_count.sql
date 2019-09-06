CREATE TEMP FUNCTION udf_js_histogram_to_threshold_count(histogram_json STRING, threshold INT64) RETURNS INT64 LANGUAGE js AS """
const histogram = JSON.parse(histogram_json).values;
let count = 0;
Object.keys(histogram).forEach(key => {if (parseInt(key) >= threshold) { count += histogram[key]; }});
return count;
""";

-- Tests
SELECT
  assert_equals(udf_js_histogram_to_threshold_count('{"values":{"1": 0, "2": 4, "8": 1}}', 3), 1),
  assert_equals(udf_js_histogram_to_threshold_count('{"values":{"1": 0, "2": 4, "8": 1}}', 2), 5)
