WITH latest_versions AS (
  SELECT channel, MAX(CAST(app_version AS INT64)) AS latest_version
  FROM
    (SELECT
      normalized_channel AS channel,
      SPLIT(application.version, '.')[OFFSET(0)] AS app_version,
      COUNT(*)
    FROM `moz-fx-data-shared-prod.telemetry.main`
    WHERE DATE(submission_timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND normalized_channel IN ("nightly", "beta", "release")
    GROUP BY 1, 2
    HAVING COUNT(*) > 1000
    ORDER BY 1, 2 DESC
    limit 100)
  GROUP BY 1),

scalar_aggregates AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    scalar_aggs.channel,
    metric,
    metric_type,
    key,
    agg_type,
    CASE agg_type
      WHEN 'max' THEN max(value)
      WHEN 'min' THEN min(value)
      WHEN 'avg' THEN avg(value)
      WHEN 'sum' THEN sum(value)
      WHEN 'false' THEN sum(value)
      WHEN 'true' THEN sum(value)
    END AS agg_value
  FROM
    clients_daily_scalar_aggregates_v1 AS scalar_aggs
  CROSS JOIN
    UNNEST(scalar_aggregates)
  LEFT JOIN latest_versions
  ON latest_versions.channel = scalar_aggs.channel
  WHERE
    value IS NOT NULL
  GROUP BY
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type
),

all_booleans AS (
  SELECT
    *
  FROM
    scalar_aggregates
  WHERE
    metric_type in ("boolean", "keyed-scalar-boolean")
),

boolean_columns AS
  (SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type,
    CASE agg_type
      WHEN 'true' THEN agg_value ELSE 0
    END AS bool_true,
    CASE agg_type
      WHEN 'false' THEN agg_value ELSE 0
    END AS bool_false
  FROM all_booleans),

summed_bools AS
  (SELECT
      client_id,
      os,
      app_version,
      app_build_id,
      channel,
      metric,
      metric_type,
      key,
      '' AS agg_type,
      SUM(bool_true) AS bool_true,
      SUM(bool_false) AS bool_false
  FROM boolean_columns
  GROUP BY 1,2,3,4,5,6,7,8,9),

booleans AS
  (SELECT * EXCEPT(bool_true, bool_false),
  CASE
    WHEN bool_true > 0 AND bool_false > 0
    THEN "sometimes"
    WHEN bool_true > 0 AND bool_false = 0
    THEN "always"
    WHEN bool_true = 0 AND bool_false > 0
    THEN "never"
  END AS agg_value
  FROM summed_bools
  WHERE bool_true > 0 OR bool_false > 0)

SELECT
  *
FROM
  booleans

UNION ALL

SELECT
  * REPLACE(CAST(agg_value AS STRING) AS agg_value)
FROM
  scalar_aggregates
WHERE
  metric_type in ("scalar", "keyed-scalar")
