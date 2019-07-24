
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
