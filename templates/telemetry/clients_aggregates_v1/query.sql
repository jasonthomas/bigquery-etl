WITH scalar_aggregates AS (
  SELECT
    * EXCEPT(value, submission_date, scalar_aggregates),
    CASE agg_type
      WHEN 'max' THEN max(value)
      WHEN 'min' THEN min(value)
      WHEN 'avg' THEN avg(value)
      WHEN 'sum' THEN sum(value)
      WHEN 'false' THEN sum(value)
      WHEN 'true' THEN sum(value)
    END AS agg_value
  FROM
    clients_daily_scalar_aggregates_v1
  CROSS JOIN
    UNNEST(scalar_aggregates)
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
