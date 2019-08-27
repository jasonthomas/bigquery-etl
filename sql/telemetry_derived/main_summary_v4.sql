CREATE TEMP FUNCTION
  udf_boolean_histogram_to_boolean(histogram STRING) AS (
    COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(histogram,
          "$.values.1") AS INT64) > 0,
      NOT SAFE_CAST( JSON_EXTRACT_SCALAR(histogram,
          "$.values.0") AS INT64) > 0));
CREATE TEMP FUNCTION
  udf_json_extract_int_map (input STRING) AS (ARRAY(
    SELECT
      STRUCT(CAST(SPLIT(entry, ':')[OFFSET(0)] AS INT64) AS key,
             CAST(SPLIT(entry, ':')[OFFSET(1)] AS INT64) AS value)
    FROM
      UNNEST(SPLIT(REPLACE(TRIM(input, '{}'), '"', ''), ',')) AS entry
    WHERE
      LENGTH(entry) > 0 ));
CREATE TEMP FUNCTION
  udf_json_extract_histogram (input STRING) AS (STRUCT(
    CAST(JSON_EXTRACT_SCALAR(input, '$.bucket_count') AS INT64) AS bucket_count,
    CAST(JSON_EXTRACT_SCALAR(input, '$.histogram_type') AS INT64) AS histogram_type,
    CAST(JSON_EXTRACT_SCALAR(input, '$.sum') AS INT64) AS `sum`,
    ARRAY(
      SELECT
        CAST(bound AS INT64)
      FROM
        UNNEST(SPLIT(TRIM(JSON_EXTRACT(input, '$.range'), '[]'), ',')) AS bound) AS `range`,
    udf_json_extract_int_map(JSON_EXTRACT(input, '$.values')) AS `values` ));
CREATE TEMP FUNCTION
  udf_enum_histogram_to_count(histogram STRING) AS ((
    SELECT
      MAX(value)
    FROM
      UNNEST(udf_json_extract_histogram(histogram).values)
    WHERE
      value > 0));
CREATE TEMP FUNCTION udf_get_key(map ANY TYPE, k ANY TYPE) AS (
 (
   SELECT key_value.value
   FROM UNNEST(map) AS key_value
   WHERE key_value.key = k
   LIMIT 1
 )
);
CREATE TEMP FUNCTION
  udf_get_plugins_notification_user_action(plugins_notification_user_action STRING) AS (
    ARRAY(
    SELECT
      AS STRUCT
      ANY_VALUE(IF(key = 0, value, NULL)) AS allow_now,
      ANY_VALUE(IF(key = 1, value, NULL)) AS allow_always,
      ANY_VALUE(IF(key = 2, value, NULL)) AS block
    FROM
      UNNEST(udf_json_extract_histogram(plugins_notification_user_action).values)));
CREATE TEMP FUNCTION
  udf_get_popup_notification_stats(popup_notification_stats ARRAY<STRUCT<key STRING, value STRING>>) AS (
    ARRAY(
    SELECT
      AS STRUCT
      key,
      (
      SELECT
        AS STRUCT
        ANY_VALUE(IF(_0.key = 0,  _0.value, NULL)) AS offered,
        ANY_VALUE(IF(_0.key = 1,  _0.value, NULL)) AS action_1,
        ANY_VALUE(IF(_0.key = 2,  _0.value, NULL)) AS action_2,
        ANY_VALUE(IF(_0.key = 3,  _0.value, NULL)) AS action_3,
        ANY_VALUE(IF(_0.key = 4,  _0.value, NULL)) AS action_last,
        ANY_VALUE(IF(_0.key = 5,  _0.value, NULL)) AS dismissal_click_elsewhere,
        ANY_VALUE(IF(_0.key = 6,  _0.value, NULL)) AS dismissal_leave_page,
        ANY_VALUE(IF(_0.key = 7,  _0.value, NULL)) AS dismissal_close_button,
        ANY_VALUE(IF(_0.key = 8,  _0.value, NULL)) AS dismissal_not_now,
        ANY_VALUE(IF(_0.key = 10, _0.value, NULL)) AS open_submenu,
        ANY_VALUE(IF(_0.key = 11, _0.value, NULL)) AS learn_more,
        ANY_VALUE(IF(_0.key = 20, _0.value, NULL)) AS reopen_offered,
        ANY_VALUE(IF(_0.key = 21, _0.value, NULL)) AS reopen_action_1,
        ANY_VALUE(IF(_0.key = 22, _0.value, NULL)) AS reopen_action_2,
        ANY_VALUE(IF(_0.key = 23, _0.value, NULL)) AS reopen_action_3,
        ANY_VALUE(IF(_0.key = 24, _0.value, NULL)) AS reopen_action_last,
        ANY_VALUE(IF(_0.key = 25, _0.value, NULL)) AS reopen_dismissal_click_elsewhere,
        ANY_VALUE(IF(_0.key = 26, _0.value, NULL)) AS reopen_dismissal_leave_page,
        ANY_VALUE(IF(_0.key = 27, _0.value, NULL)) AS reopen_dismissal_close_button,
        ANY_VALUE(IF(_0.key = 28, _0.value, NULL)) AS reopen_dismissal_not_now,
        ANY_VALUE(IF(_0.key = 30, _0.value, NULL)) AS reopen_open_submenu,
        ANY_VALUE(IF(_0.key = 31, _0.value, NULL)) AS reopen_learn_more
      FROM
        UNNEST(udf_json_extract_histogram(value).values) AS _0) AS value
    FROM
      UNNEST(popup_notification_stats)));
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
CREATE TEMP FUNCTION
  udf_js_get_active_addons(active_addons ARRAY<STRUCT<key STRING,
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
    active_addons_json STRING)
  RETURNS ARRAY<STRUCT< addon_id STRING,
  blocklisted BOOL,
  name STRING,
  user_disabled BOOL,
  app_disabled BOOL,
  version STRING,
  scope INT64,
  type STRING,
  foreign_install BOOL,
  has_binary_components BOOL,
  install_day INT64,
  update_day INT64,
  is_system BOOL,
  is_web_extension BOOL,
  multiprocess_compatible BOOL>>
  LANGUAGE js AS """
var additional_properties = JSON.parse(active_addons_json);
var result = [];
active_addons.forEach((item) => {
  var addon_json = additional_properties[item.key];
  if (addon_json === undefined) {
    addon_json = {};
  }
  var value = item.value;
  if (value === undefined) {
    value = {};
  }
  result.push({
    "addon_id": item.key,
    "blocklisted": value.blocklisted,
    "name": value.name,
    "user_disabled": addon_json.userDisabled,
    "app_disabled": value.app_disabled,
    "version": addon_json.version,
    "scope": value.scope,
    "type": value.type,
    "foreign_install": addon_json.foreignInstall,
    "has_binary_components": value.has_binary_components,
    "install_day": value.install_day,
    "update_day": value.update_day,
    "is_system": value.is_system,
    "is_web_extension": addon_json.isWebExtension,
    "multiprocess_compatible": addon_json.multiprocessCompatible,
  });
});
return result;
""";
CREATE TEMP FUNCTION
  udf_max_flash_version(active_plugins ANY TYPE) AS ((
    SELECT
      AS STRUCT
      version,
      SAFE_CAST(parts[SAFE_OFFSET(0)] AS INT64) AS major,
      SAFE_CAST(parts[SAFE_OFFSET(1)] AS INT64) AS minor,
      SAFE_CAST(parts[SAFE_OFFSET(2)] AS INT64) AS patch,
      SAFE_CAST(parts[SAFE_OFFSET(3)] AS INT64) AS build
    FROM
      UNNEST(active_plugins),
      UNNEST([STRUCT(SPLIT(version, ".") AS parts)])
    WHERE
      name = "Shockwave Flash"
    ORDER BY
      major DESC,
      minor DESC,
      patch DESC,
      build DESC
    LIMIT
      1).version);
--
SELECT
  document_id,
  client_id,
  sample_id,
  metadata.uri.app_update_channel AS channel,
  normalized_channel,
  normalized_os_version,
  metadata.geo.country,
  metadata.geo.city,
  metadata.geo.subdivision1 AS geo_subdivision1,
  metadata.geo.subdivision2 AS geo_subdivision2,
  environment.system.os.name AS os,
  JSON_EXTRACT_SCALAR(additional_properties, "$.environment.system.os.version") AS os_version,
  SAFE_CAST(environment.system.os.service_pack_major AS INT64) AS os_service_pack_major,
  SAFE_CAST(environment.system.os.service_pack_minor AS INT64) AS os_service_pack_minor,
  SAFE_CAST(environment.system.os.windows_build_number AS INT64) AS windows_build_number,
  SAFE_CAST(environment.system.os.windows_ubr AS INT64) AS windows_ubr,

  -- Note: Windows only!
  SAFE_CAST(environment.system.os.install_year AS INT64) AS install_year,
  environment.system.is_wow64,

  SAFE_CAST(environment.system.memory_mb AS INT64) AS memory_mb,

  environment.system.cpu.count AS cpu_count,
  environment.system.cpu.cores AS cpu_cores,
  environment.system.cpu.vendor AS cpu_vendor,
  environment.system.cpu.family AS cpu_family,
  environment.system.cpu.model AS cpu_model,
  environment.system.cpu.stepping AS cpu_stepping,
  SAFE_CAST(environment.system.cpu.l2cache_kb AS INT64) AS cpu_l2_cache_kb,
  SAFE_CAST(environment.system.cpu.l3cache_kb AS INT64) AS cpu_l3_cache_kb,
  SAFE_CAST(environment.system.cpu.speed_m_hz AS INT64) AS cpu_speed_mhz,

  environment.system.gfx.features.d3d11.status AS gfx_features_d3d11_status,
  environment.system.gfx.features.d2d.status AS gfx_features_d2d_status,
  environment.system.gfx.features.gpu_process.status AS gfx_features_gpu_process_status,
  environment.system.gfx.features.advanced_layers.status AS gfx_features_advanced_layers_status,
  JSON_EXTRACT_SCALAR(additional_properties, "$.environment.system.gfx.features.wr_qualified.status") AS gfx_features_wrqualified_status,
  JSON_EXTRACT_SCALAR(additional_properties, "$.environment.system.gfx.features.webrender.status") AS gfx_features_webrender_status,

  -- Bug 1552940
  environment.system.hdd.profile.type AS hdd_profile_type,
  environment.system.hdd.binary.type AS hdd_binary_type,
  environment.system.hdd.system.type AS hdd_system_type,

  environment.system.apple_model_id,

  -- Bug 1431198 - Windows 8 only
  environment.system.sec.antivirus,
  environment.system.sec.antispyware,
  environment.system.sec.firewall,

  -- TODO: use proper 'date' type for date columns.
  SAFE_CAST(environment.profile.creation_date AS INT64) AS profile_creation_date,
  SAFE_CAST(environment.profile.reset_date AS INT64) AS profile_reset_date,
  JSON_EXTRACT_SCALAR(additional_properties, "$.payload.info.previous_build_id") AS previous_build_id,
  JSON_EXTRACT_SCALAR(additional_properties, "$.payload.info.session_id") AS session_id,
  JSON_EXTRACT_SCALAR(additional_properties, "$.payload.info.subsession_id") AS subsession_id,
  JSON_EXTRACT_SCALAR(additional_properties, "$.payload.info.previous_subsession_id") AS previous_subsession_id,
  JSON_EXTRACT_SCALAR(additional_properties, "$.payload.info.session_start_date") AS session_start_date,
  SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.info.session_length") AS INT64) AS session_length,
  payload.info.subsession_length,
  SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.info.subsession_counter") AS INT64) AS subsession_counter,
  SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.info.profile_subsession_counter") AS INT64) AS profile_subsession_counter,
  creation_date,
  environment.partner.distribution_id,
  DATE(submission_timestamp) AS submission_date,
  -- See bug 1550752
  udf_boolean_histogram_to_boolean(payload.histograms.fxa_configured) AS fxa_configured,
  -- See bug 1232050
  udf_boolean_histogram_to_boolean(payload.histograms.weave_configured) AS sync_configured,
  udf_enum_histogram_to_count(payload.histograms.weave_device_count_desktop) AS sync_count_desktop,
  udf_enum_histogram_to_count(payload.histograms.weave_device_count_mobile) AS sync_count_mobile,

  application.build_id AS app_build_id,
  application.display_version AS app_display_version,
  application.name AS app_name,
  application.version AS app_version,
  UNIX_MICROS(submission_timestamp) * 1000 AS `timestamp`,

  environment.build.build_id AS env_build_id,
  environment.build.version AS env_build_version,
  environment.build.architecture AS env_build_arch,

  -- See bug 1232050
  environment.settings.e10s_enabled,

  -- See bug 1232050
  environment.settings.e10s_multi_processes,

  environment.settings.locale,
  environment.settings.update.channel AS update_channel,
  environment.settings.update.enabled AS update_enabled,
  environment.settings.update.auto_download AS update_auto_download,
  STRUCT(environment.settings.attribution.source, environment.settings.attribution.medium, environment.settings.attribution.campaign, environment.settings.attribution.content) AS attribution,
  environment.settings.sandbox.effective_content_process_level AS sandbox_effective_content_process_level,
  environment.addons.active_experiment.id AS active_experiment_id,
  environment.addons.active_experiment.branch AS active_experiment_branch,
  JSON_EXTRACT_SCALAR(additional_properties, "$.payload.info.reason") AS reason,

  SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.info.timezone_offset") AS INT64) AS timezone_offset,

  -- Different types of crashes / hangs:
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_crashes_with_dump, "pluginhang")).sum AS plugin_hangs,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_abnormal_abort, "plugin")).sum AS aborts_plugin,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_abnormal_abort, "content")).sum AS aborts_content,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_abnormal_abort, "gmplugin")).sum AS aborts_gmplugin,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_crashes_with_dump, "plugin")).sum AS crashes_detected_plugin,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_crashes_with_dump, "content")).sum AS crashes_detected_content,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_crashes_with_dump, "gmplugin")).sum AS crashes_detected_gmplugin,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_attempt, "main_crash")).sum AS crash_submit_attempt_main,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_attempt, "content_crash")).sum AS crash_submit_attempt_content,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_attempt, "plugin_crash")).sum AS crash_submit_attempt_plugin,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_success, "main_crash")).sum AS crash_submit_success_main,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_success, "content_crash")).sum AS crash_submit_success_content,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_success, "plugin_crash")).sum AS crash_submit_success_plugin,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_kill_hard, "shut_down_kill")).sum AS shutdown_kill,

  ARRAY_LENGTH(environment.addons.active_addons) AS active_addons_count,

  -- See https://github.com/mozilla-services/data-pipeline/blob/master/hindsight/modules/fx/ping.lua#L82
  udf_max_flash_version(environment.addons.active_plugins) AS flash_version, -- latest installable version of flash plugin.
  application.vendor,
  environment.settings.is_default_browser,
  environment.settings.default_search_engine_data.name AS default_search_engine_data_name,
  environment.settings.default_search_engine_data.load_path AS default_search_engine_data_load_path,
  environment.settings.default_search_engine_data.origin AS default_search_engine_data_origin,
  environment.settings.default_search_engine_data.submission_url AS default_search_engine_data_submission_url,
  environment.settings.default_search_engine,

  -- DevTools usage per bug 1262478
  udf_json_extract_histogram(payload.histograms.devtools_toolbox_opened_count).sum AS devtools_toolbox_opened_count,

  -- client date per bug 1270505
  metadata.header.date AS client_submission_date, -- the HTTP Date header sent by the client

  -- clock skew per bug 1270183
  TIMESTAMP_DIFF(SAFE.PARSE_TIMESTAMP("%a, %d %b %Y %T %Z", metadata.header.date), submission_timestamp, SECOND) AS client_clock_skew,
  TIMESTAMP_DIFF(SAFE.PARSE_TIMESTAMP("%FT%R:%E*SZ", creation_date), submission_timestamp, SECOND) AS client_submission_latency,

  -- We use the mean for bookmarks and pages because we do not expect them to be
  -- heavily skewed during the lifetime of a subsession. Using the median for a
  -- histogram would probably be better in general, but the granularity of the
  -- buckets for these particular histograms is not fine enough for the median
  -- to give a more accurate value than the mean.
  (SELECT SAFE_CAST(AVG(value) AS INT64) FROM UNNEST(udf_json_extract_histogram(payload.histograms.places_bookmarks_count).values)) AS places_bookmarks_count,
  (SELECT SAFE_CAST(AVG(value) AS INT64) FROM UNNEST(udf_json_extract_histogram(payload.histograms.places_pages_count).values)) AS places_pages_count,

  -- Push metrics per bug 1270482 and bug 1311174
  udf_json_extract_histogram(payload.histograms.push_api_notify).sum AS push_api_notify,
  udf_json_extract_histogram(payload.histograms.web_notification_shown).sum AS web_notification_shown,

  -- Info from POPUP_NOTIFICATION_STATS keyed histogram
  udf_get_popup_notification_stats(payload.keyed_histograms.popup_notification_stats) AS popup_notification_stats,

  -- Search counts
  -- split up and organize the SEARCH_COUNTS keyed histogram
  udf_get_search_counts(payload.keyed_histograms.search_counts) AS search_counts,

  -- Addon and configuration settings per Bug 1290181
  udf_js_get_active_addons(environment.addons.active_addons, JSON_EXTRACT(additional_properties, "$.environment.addons.activeAddons")) AS active_addons,

  -- Legacy/disabled addon and configuration settings per Bug 1390814. Please note that |disabled_addons_ids| may go away in the future.
  udf_js_get_disabled_addons(environment.addons.active_addons, JSON_EXTRACT(additional_properties, "$.payload.addonDetails") AS disabled_addons_ids, -- One per item in payload.addonDetails.XPI
  udf_get_theme(environment.addons.theme) AS active_theme,
  environment.settings.blocklist_enabled,
  environment.settings.addon_compatibility_check_enabled,
  environment.settings.telemetry_enabled,

  environment.settings.intl.accept_languages AS environment_settings_intl_accept_languages,
  environment.settings.intl.app_locales AS environment_settings_intl_app_locales,
  environment.settings.intl.available_locales AS environment_settings_intl_available_locales,
  environment.settings.intl.regional_prefs_locales AS environment_settings_intl_regional_prefs_locales,
  environment.settings.intl.requested_locales AS environment_settings_intl_requested_locales,
  environment.settings.intl.system_locales AS environment_settings_intl_system_locales,

  environment.system.gfx.headless AS environment_system_gfx_headless,

  -- TODO: Deprecate and eventually remove this field, preferring the top-level user_pref_* fields for easy schema evolution.
udf_get_old_user_prefs(environment.settings.user_prefs) AS user_prefs,

udf_js_get_events([
  ("content", JSON_EXTRACT(additional_properties, "payload.processes.content.events")),
  ("dynamic", JSON_EXTRACT(additional_properties, "payload.processes.dynamic.events")),
  ("gpu", JSON_EXTRACT(additional_properties, "payload.processes.gpu.events")),
  ("parent", JSON_EXTRACT(additional_properties, "payload.processes.parent.events"))]) AS events,

  -- bug 1339655
  SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.ssl_handshake_result, "$.values.0") AS INT64) AS ssl_handshake_result_success,
  (SELECT SUM(value) FROM UNNEST(udf_json_extract_histogram(payload.histograms.ssl_handshake_result).values) WHERE key BETWEEN 1 AND 671) AS ssl_handshake_result_failure,
  (SELECT STRUCT(CAST(key AS STRING) AS key, value) FROM UNNEST(udf_json_extract_histogram(payload.histograms.ssl_handshake_result).values) WHERE key BETWEEN 0 AND 671) AS ssl_handshake_result,

  -- bug 1353114 - payload.simpleMeasurements.*
  COALESCE(
    payload.processes.parent.scalars.browser_engagement_active_ticks,
    SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.simple_measurements.active_ticks") AS INT64)) AS active_ticks,
  SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.simple_measurements.main") AS INT64) AS main,
  COALESCE(
    payload.processes.parent.scalars.timestamps_first_paint,
    SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.simple_measurements.first_paint") AS INT64)) AS first_paint,
  SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.simple_measurements.session_restored") AS INT64) AS session_restored,
  SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.simple_measurements.total_time") AS INT64) AS total_time,
  SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.simple_measurements.blank_window_shown") AS INT64) AS blank_window_shown,

  -- bug 1362520 and 1526278 - plugin notifications
  SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.plugins_notification_user_action, "$.values.1") AS INT64) AS plugins_notification_shown,
  SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.plugins_notification_user_action, "$.values.0") AS INT64) AS plugins_notification_false,
  udf_get_plugins_notification_user_action(payload.histograms.plugins_notification_user_action) AS plugins_notification_user_action,
  udf_json_extract_histogram(payload.histograms.plugins_infobar_shown).sum AS plugins_infobar_shown,
  udf_json_extract_histogram(payload.histograms.plugins_infobar_block).sum AS plugins_infobar_block,
  udf_json_extract_histogram(payload.histograms.plugins_infobar_allow).sum AS plugins_infobar_allow,
  udf_json_extract_histogram(payload.histograms.plugins_infobar_dismissed).sum AS plugins_infobar_dismissed,

  -- bug 1366253 - active experiments
udf_get_experiments(JSON_EXTRACT(additional_properties, "$.environment.experiments"))), -- experiment id->branchname

  environment.settings.search_cohort,

  -- bug 1366838 - Quantum Release Criteria
  environment.system.gfx.features.compositor AS gfx_compositor --,
  udf_get_quantum_ready(environment.settings.e10s_enabled, environment.addons.active_addons, JSON_EXTRACT(additional_properties, "$.environment.addons.activeAddons"), environment.addons.theme) AS quantum_ready,

--udf_histogram_to_threshold_count(payload.histograms.gc_max_pause_ms_2, 150),
--udf_histogram_to_threshold_count(payload.histograms.gc_max_pause_ms_2, 250),
--udf_histogram_to_threshold_count(payload.histograms.gc_max_pause_ms_2, 2500),

--udf_histogram_to_threshold_count(payload.processes.content.histograms.gc_max_pause_ms_2, 150),
--udf_histogram_to_threshold_count(payload.processes.content.histograms.gc_max_pause_ms_2, 250),
--udf_histogram_to_threshold_count(payload.processes.content.histograms.gc_max_pause_ms_2, 2500),

--udf_histogram_to_threshold_count(payload.histograms.cycle_collector_max_pause, 150),
--udf_histogram_to_threshold_count(payload.histograms.cycle_collector_max_pause, 250),
--udf_histogram_to_threshold_count(payload.histograms.cycle_collector_max_pause, 2500),

--udf_histogram_to_threshold_count(payload.processes.content.histograms.cycle_collector_max_pause, 150),
--udf_histogram_to_threshold_count(payload.processes.content.histograms.cycle_collector_max_pause, 250),
--udf_histogram_to_threshold_count(payload.processes.content.histograms.cycle_collector_max_pause, 2500),

--udf_histogram_to_threshold_count(payload.histograms.input_event_response_coalesced_ms, 150),
--udf_histogram_to_threshold_count(payload.histograms.input_event_response_coalesced_ms, 250),
--udf_histogram_to_threshold_count(payload.histograms.input_event_response_coalesced_ms, 2500),

--udf_histogram_to_threshold_count(payload.processes.content.histograms.input_event_response_coalesced_ms, 150),
--udf_histogram_to_threshold_count(payload.processes.content.histograms.input_event_response_coalesced_ms, 250),
--udf_histogram_to_threshold_count(payload.processes.content.histograms.input_event_response_coalesced_ms, 2500),

--udf_histogram_to_threshold_count(payload.histograms.ghost_windows, 1),
--udf_histogram_to_threshold_count(payload.processes.content.histograms.ghost_windows, 1),

  /*
  TODO
  udf_get_user_prefs(environment.settings.user_prefs,
    [("bool", "browser.launcherProcess.enabled"),
    ("bool", "browser.search.widget.inNavBar"),
    ("string", "browser.search.region"),
    ("bool", "extensions.allow-non-mpc-extensions"),
    ("bool", "extensions.legacy.enabled"),
    ("bool", "gfx.webrender.all.qualified"),
    ("bool", "marionette.enabled"),
    ("bool", "privacy.fuzzyfox.enabled"),
    ("int", "dom.ipc.plugins.sandbox-level.flash"),
    ("int", "dom.ipc.processCount"),
    ("string", "general.config.filename"),
    ("bool", "security.enterprise_roots.auto-enabled"),
    ("bool", "security.enterprise_roots.enabled"),
    ("bool", "security.pki.mitm_detected")]).*,
  udf_scalar_row(STRUCT(
    STRUCT(
      payload.processes.content.scalars AS content,
      payload.processes.gpu.scalars AS gpu,
      payload.scalars AS parent
    ) AS scalars,
    STRUCT(
      payload.processes.content.keyed_scalars AS content,
      payload.processes.gpu.keyed_scalars AS gpu,
      payload.keyed_scalars AS parent
    ) AS keyed_scalars,
    ARRAY(SELECT * FROM scalarDefinitions WHERE process != "dynamic") AS scalarDefinitions)).*,
  udf_histogram_row(STRUCT(
    STRUCT(
      payload.processes.content.histograms AS content,
      payload.processes.gpu.histograms AS gpu,
      payload.histograms AS parent
    ) AS histograms,
    STRUCT(
      payload.processes.content.keyed_histograms AS content,
      payload.processes.gpu.keyed_histograms AS gpu,
      payload.keyed_histograms AS parent
    ) AS keyed_histograms,
    [] AS histogramDefinitions,
    [] AS naturalHistogramRepresentationList)).*,
  udf_addon_scalars_row(STRUCT(
    payload.processes.dynamic.scalars AS addon_scalars,
    payload.processes.dynamic.keyed_scalars AS addon_keyed_scalars,
    ARRAY(SELECT * FROM scalarDefinitions WHERE process != "dynamic") AS scalarDefinitions)).*
  */
FROM
  `moz-fx-data-shared-prod.telemetry_stable.main_v4`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND normalized_app_name = "Firefox"
  AND document_id IS NOT NULL
