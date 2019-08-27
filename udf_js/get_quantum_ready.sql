CREATE TEMP FUNCTION
  udf_js_get_quantum_ready(e10s_enabled BOOL, active_addons ARRAY<STRUCT<key STRING,
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
  active_addons_json STRING,
  theme STRUCT<id STRING,
    blocklisted BOOL,
    description STRING,
    name STRING,
    user_disabled BOOL,
    app_disabled BOOL,
    version STRING,
    scope INT64,
    foreign_install BOOL,
    has_binary_components BOOL,
    install_day INT64,
    update_day INT64>)
    RETURNS BOOL
  LANGUAGE js AS """
    const activeAddonsExtras = JSON.parse(active_addons_json);
    return (e10s_enabled &&
            active_addons.every(a => a.value.is_system || (activeAddonsExtras[a.key] && activeAddonsExtras[a.key].isWebExtension)) &&
            ["{972ce4c6-7e08-4474-a285-3208198ce6fd}",
             "firefox-compact-light@mozilla.org",
             "firefox-compact-dark@mozilla.org"].includes(theme.id));
  """;

-- Tests

SELECT
  assert_true(udf_js_get_quantum_ready(
   true,
   [STRUCT("addon_id", STRUCT(true, true, "", true, 2, false, "name", 1, 0, "type", 3))],
   '{"addon_id":{"multiprocessCompatible":true,"isWebExtension":true,"version":"version","userDisabled":true,"foreignInstall":true}}',
    STRUCT("firefox-compact-light@mozilla.org", false, "desc", "example theme", false, false, "1.0.1", 0, true, true, 7490, 7551))),
  assert_true(udf_js_get_quantum_ready(
   true,
   [STRUCT("addon_id", STRUCT(true, true, "", true, 2, true, "name", 1, 0, "type", 3))],
   '{"addon_id":{"multiprocessCompatible":true,"isWebExtension":false,"version":"version","userDisabled":true,"foreignInstall":true}}',
    STRUCT("firefox-compact-light@mozilla.org", false, "desc", "example theme", false, false, "1.0.1", 0, true, true, 7490, 7551))),
  assert_false(udf_js_get_quantum_ready(
   true,
   [STRUCT("addon_id", STRUCT(true, true, "", true, 2, false, "name", 1, 0, "type", 3))],
   '{"wrong_id":{"multiprocessCompatible":true,"isWebExtension":true,"version":"version","userDisabled":true,"foreignInstall":true}}',
    STRUCT("firefox-compact-light@mozilla.org", false, "desc", "example theme", false, false, "1.0.1", 0, true, true, 7490, 7551))),
  assert_false(udf_js_get_quantum_ready(
   false,
   [STRUCT("addon_id", STRUCT(true, true, "", true, 2, true, "name", 1, 0, "type", 3))],
   '{"wrong_id":{"multiprocessCompatible":true,"isWebExtension":true,"version":"version","userDisabled":true,"foreignInstall":true}}',
    STRUCT("firefox-compact-light@mozilla.org", false, "desc", "example theme", false, false, "1.0.1", 0, true, true, 7490, 7551))),
  assert_false(udf_js_get_quantum_ready(
   true,
   [STRUCT("addon_id", STRUCT(true, true, "", true, 2, true, "name", 1, 0, "type", 3))],
   '{"addon_id":{"multiprocessCompatible":true,"isWebExtension":true,"version":"version","userDisabled":true,"foreignInstall":true}}',
    STRUCT("wrong-theme", false, "desc", "example theme", false, false, "1.0.1", 0, true, true, 7490, 7551))),
  assert_false(udf_js_get_quantum_ready(
   true,
   [STRUCT("addon_id", STRUCT(true, true, "", true, 2, true, "name", 1, 0, "type", 3)), STRUCT("addon_id2", STRUCT(true, true, "", true, 2, false, "name", 1, 0, "type", 3))],
   '{"addon_id":{"multiprocessCompatible":true,"isWebExtension":true,"version":"version","userDisabled":true,"foreignInstall":true}, "addon_id2":{"multiprocessCompatible":true,"isWebExtension":false,"version":"version","userDisabled":true,"foreignInstall":true}}',
    STRUCT("firefox-compact-light@mozilla.org", false, "desc", "example theme", false, false, "1.0.1", 0, true, true, 7490, 7551))),
  assert_true(udf_js_get_quantum_ready(
   true,
   [STRUCT("addon_id", STRUCT(true, true, "", true, 2, true, "name", 1, 0, "type", 3)), STRUCT("addon_id2", STRUCT(true, true, "", true, 2, false, "name", 1, 0, "type", 3))],
   '{"addon_id":{"multiprocessCompatible":true,"isWebExtension":false,"version":"version","userDisabled":true,"foreignInstall":true}, "addon_id2":{"multiprocessCompatible":true,"isWebExtension":true,"version":"version","userDisabled":true,"foreignInstall":true}}',
    STRUCT("firefox-compact-light@mozilla.org", false, "desc", "example theme", false, false, "1.0.1", 0, true, true, 7490, 7551)))
