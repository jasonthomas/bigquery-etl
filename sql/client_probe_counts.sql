CREATE TEMP FUNCTION udf_bucket (val FLOAT64, min_bucket INT64, max_bucket INT64, num_buckets INT64)
RETURNS FLOAT64 AS (
  -- Bucket `value` into a histogram with min_bucket, max_bucket and num_buckets
  (
    SELECT
      max(bucket)
    FROM
      unnest(GENERATE_ARRAY(min_bucket, max_bucket, (max_bucket - min_bucket) / num_buckets)) AS bucket
    WHERE
      val > bucket
  )
);

CREATE TEMP FUNCTION udf_dedupe_map_sum (map STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>)
RETURNS STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>> AS (
  -- Given a MAP with duplicate keys, de-duplicates by summing the values of duplicate keys
  (
    WITH summed_counts AS (
      SELECT
        STRUCT<key FLOAT64, value FLOAT64>(e.key, SUM(e.value)) AS record
      FROM
        UNNEST(map.key_value) AS e
      GROUP BY
        e.key
    )

    SELECT
      STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>(
        ARRAY_AGG(record)
      )
    FROM
      summed_counts
  )
);

CREATE TEMP FUNCTION udf_buckets_to_map (buckets ARRAY<FLOAT64>)
RETURNS STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>> AS (
  -- Given an array of values, transform them into a histogram MAP
  -- with the number of each key in the `buckets` array
  (
    SELECT
      STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>(
        ARRAY_AGG(STRUCT<key FLOAT64, value FLOAT64>(bucket, 1.0))
      )
    FROM
      UNNEST(buckets) AS bucket
  )
);

CREATE TEMP FUNCTION udf_fill_buckets(input_map STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>, min_bucket INT64, max_bucket INT64, num_buckets INT64)
RETURNS STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>> AS (
  -- Given a MAP `input_map`, fill in any missing keys with value `0.0`
  (
    WITH total_counts AS (
      SELECT
        key,
        COALESCE(e.value, 0.0) AS value
      FROM
        UNNEST(GENERATE_ARRAY(min_bucket, max_bucket, (max_bucket - min_bucket) / num_buckets)) as key
      LEFT JOIN
        UNNEST(input_map.key_value) AS e ON key = e.key
    )
    
    SELECT
      STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>(
        ARRAY_AGG(STRUCT<key FLOAT64, value FLOAT64>(key, value))
      )
    FROM
      total_counts
  )
);

WITH bucketed_scalars AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    agg_type,
    min_bucket,
    max_bucket,
    num_buckets,
    udf_bucket(agg_value, min_bucket, max_bucket, num_buckets) AS bucket
  FROM
    telemetry.clients_aggregates_v1
)


SELECT
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  agg_type,
  udf_fill_buckets(
    udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
    0, 1000, 50
  ) AS aggregates
FROM
  bucketed_scalars
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  CAST(NULL AS STRING) as os,
  app_version,
  app_build_id,
  channel,
  metric,
  agg_type,
  udf_fill_buckets(
    udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
    0, 1000, 50
  ) AS aggregates
FROM
  bucketed_scalars
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  app_build_id,
  channel,
  metric,
  agg_type,
  udf_fill_buckets(
    udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
    0, 1000, 50
  ) AS aggregates
FROM
  bucketed_scalars
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  agg_type,
  udf_fill_buckets(
    udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
    0, 1000, 50
  ) AS aggregates
FROM
  bucketed_scalars
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  agg_type,
  udf_fill_buckets(
    udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
    0, 1000, 50
  ) AS aggregates
FROM
  bucketed_scalars
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  agg_type,
  udf_fill_buckets(
    udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
    0, 1000, 50
  ) AS aggregates
FROM
  bucketed_scalars
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  agg_type,
  udf_fill_buckets(
    udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
    0, 1000, 50
  ) AS aggregates
FROM
  bucketed_scalars
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  agg_type,
  udf_fill_buckets(
    udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
    0, 1000, 50
  ) AS aggregates
FROM
  bucketed_scalars
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  agg_type,
  udf_fill_buckets(
    udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
    0, 1000, 50
  ) AS aggregates
FROM
  bucketed_scalars
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  agg_type,
  udf_fill_buckets(
    udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
    0, 1000, 50
  ) AS aggregates
FROM
  bucketed_scalars
GROUP BY 1, 2, 3, 4, 5, 6
