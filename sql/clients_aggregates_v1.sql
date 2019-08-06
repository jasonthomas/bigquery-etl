WITH scalars_and_booleans AS
  -- Aggregate per client without date.
  (SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    aggregate.metric as metric,
    aggregate.metric_type AS metric_type,
    aggregate.key AS key,
    aggregate.agg_type as agg_type,
    CASE agg_type
      WHEN 'max' THEN max(value) OVER w1
      WHEN 'min' THEN min(value) OVER w1
      WHEN 'avg' THEN avg(value) OVER w1
      WHEN 'sum' THEN sum(value) OVER w1
      WHEN 'false' THEN sum(value) OVER w1
      WHEN 'true' THEN sum(value) OVER w1
    END AS agg_value
  FROM
    telemetry.clients_daily_scalar_aggregates_v1
  CROSS JOIN
    UNNEST(scalar_aggregates) AS aggregate
  WHERE value IS NOT NULL
  WINDOW
      -- Aggregations require a framed window
      w1 AS (
          PARTITION BY
              client_id,
              os,
              app_version,
              app_build_id,
              channel,
              aggregate.metric,
              aggregate.metric_type,
              aggregate.key,
              aggregate.agg_type)
  ),

keyed_scalars AS
  (SELECT
      client_id,
      os,
      app_version,
      app_build_id,
      channel,
      aggregate.metric as metric,
      aggregate.metric_type AS metric_type,
      aggregate.key AS key,
      aggregate.agg_type as agg_type,
      CASE agg_type
        WHEN 'max' THEN max(value) OVER w1
        WHEN 'min' THEN min(value) OVER w1
        WHEN 'avg' THEN avg(value) OVER w1
        WHEN 'sum' THEN sum(value) OVER w1
        WHEN 'false' THEN sum(value) OVER w1
        WHEN 'true' THEN sum(value) OVER w1
      END AS agg_value
    FROM
      telemetry.clients_daily_keyed_scalar_aggregates_v1
    CROSS JOIN
      UNNEST(scalar_aggregates) AS aggregate
    WHERE value IS NOT NULL
    WINDOW
        -- Aggregations require a framed window
        w1 AS (
            PARTITION BY
                client_id,
                os,
                app_version,
                app_build_id,
                channel,
                aggregate.metric,
                aggregate.metric_type,
                aggregate.key,
                aggregate.agg_type)
  ),

all_booleans AS
    (SELECT *
    FROM scalars_and_booleans
    WHERE metric_type = "boolean"

    UNION ALL

    SELECT *
    FROM keyed_scalars
    WHERE metric_type = "keyed-scalar-boolean"),

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
      SUM(bool_true) OVER w1 AS bool_true,
      SUM(bool_false) OVER w1 AS bool_false
  FROM boolean_columns
  WINDOW
    -- Aggregations require a framed window
    w1 AS (
        PARTITION BY
            client_id,
            os,
            app_version,
            app_build_id,
            channel,
            metric,
            metric_type,
            key,
            agg_type)),

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
  * EXCEPT(agg_value),
  CAST(agg_value AS STRING) AS agg_value
FROM scalars_and_booleans
WHERE metric_type = "scalar"

UNION ALL

SELECT *
FROM booleans

UNION ALL

SELECT * EXCEPT(agg_value),
  CAST(agg_value AS STRING) AS agg_value
FROM keyed_scalars
WHERE metric_type = "keyed-scalar"
