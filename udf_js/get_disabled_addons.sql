CREATE TEMP FUNCTION
  udf_js_get_disabled_addons(active_addons ARRAY<STRUCT<key STRING,
    value STRUCT<app_disabled BOOL,
    blocklisted BOOL,
    description STRING,
    has_binary_components BOOL,
    install_day INT64,
    is_system BOOL,
    name STRING,
    scope INT64,
    signed_state INT64,
    type STRING,
    update_day INT64>>>,
    addon_details_json STRING)
  RETURNS ARRAY<STRING>
  LANGUAGE js AS """
const addonDetails = JSON.parse(addon_details_json);
const activeIds = active_addons.map(item => item.key);
let result = [];
if (addonDetails !== undefined) {
  result = addonDetails.filter(k => activeIds.includes(k));
}
return result;
""";

-- Tests

SELECT
  assert_array_equals(
    udf_js_get_disabled_addons([
      STRUCT("foo", STRUCT(false, false, "desc0", false, 1, false, "Foo", 0, 0, "addon", 1)),
      STRUCT("baz", STRUCT(false, false, "desc1", false, 1, false, "Baz", 0, 0, "addon", 1)),
      STRUCT("buz", STRUCT(false, false, "desc2", false, 1, false, "Buz", 0, 0, "addon", 1))],
      '["foo", "buz", "blee"]'),
    ["foo", "buz"])
