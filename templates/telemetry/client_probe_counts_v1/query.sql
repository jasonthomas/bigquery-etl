CREATE TEMP FUNCTION udf_exponential_buckets(dmin INT64, dmax INT64, nBuckets INT64)
RETURNS ARRAY<FLOAT64>
LANGUAGE js AS
'''
  let logMax = Math.log(dmax);
  let current = dmin;
  let retArray = [0, current];
  for (let bucketIndex = 2; bucketIndex < nBuckets; bucketIndex++) {
    let logCurrent = Math.log(current);
    let logRatio = (logMax - logCurrent) / (nBuckets - bucketIndex);
    let logNext = logCurrent + logRatio;
    let nextValue = parseInt(Math.floor(Math.exp(logNext) + 0.5));
    if (nextValue > current) {
      current = nextValue;
    } else {
      current++;
    }
    retArray[bucketIndex] = current;
  }
  return retArray
''';

CREATE TEMP FUNCTION udf_normalized_sum (arrs STRUCT<key_value ARRAY<STRUCT<key STRING, value INT64>>>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Returns the normalized sum of the input maps.
  -- It returns the total_count[k] / SUM(total_count)
  -- for each key k.
  (
    WITH total_counts AS (
      SELECT
        sum(a.value) AS total_count
      FROM
        UNNEST(arrs.key_value) AS a
    ),

    summed_counts AS (
      SELECT
        a.key AS k,
        SUM(a.value) AS v
      FROM
        UNNEST(arrs.key_value) AS a
      GROUP BY
        a.key
    ),

    final_values AS (
      SELECT
        STRUCT<key STRING, value FLOAT64>(k, 1.0 * v / total_count) AS record
      FROM
        summed_counts
      CROSS JOIN
        total_counts
    )

    SELECT
        ARRAY_AGG(record)
    FROM
      final_values
  )
);

CREATE TEMP FUNCTION udf_bucket (
  val FLOAT64,
  min_bucket INT64,
  max_bucket INT64,
  num_buckets INT64,
  metric_type STRING
)
RETURNS FLOAT64 AS (
  -- Bucket `value` into a histogram with min_bucket, max_bucket and num_buckets
  (WITH
    buckets AS (
      SELECT
        CASE
          WHEN metric_type = 'histogram-exponential'
          THEN udf_exponential_buckets(min_bucket, max_bucket, num_buckets)
          ELSE GENERATE_ARRAY(min_bucket, max_bucket,
            CASE
              WHEN ROUND((max_bucket - min_bucket) / num_buckets) < 1 THEN 1
              ELSE ROUND((max_bucket - min_bucket) / num_buckets)
            END)
        END AS bucket_arr
    )

    SELECT max(bucket)
    FROM buckets
    CROSS JOIN unnest(buckets.bucket_arr) AS bucket
    WHERE val >= bucket
  )
);

CREATE TEMP FUNCTION udf_get_buckets(min INT64, max INT64, num INT64, metric_type STRING)
RETURNS ARRAY<STRING> AS (
  (
    WITH buckets AS (
      SELECT
        CASE
          WHEN metric_type = 'histogram-exponential'
          THEN udf_exponential_buckets(min, max, num)
          ELSE
            GENERATE_ARRAY(min, max,
            CASE
              WHEN ROUND((max - min) / num) < 1 THEN 1
              ELSE ROUND((max - min) / num)
            END)
       END AS arr
    )

    SELECT ARRAY_AGG(CAST(item AS STRING))
    FROM buckets
    CROSS JOIN UNNEST(arr) AS item
  )
);

CREATE TEMP FUNCTION udf_dedupe_map_sum (map ARRAY<STRUCT<key STRING, value FLOAT64>>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Given a MAP with duplicate keys, de-duplicates by summing the values of duplicate keys
  (
    WITH summed_counts AS (
      SELECT
        STRUCT<key STRING, value FLOAT64>(e.key, SUM(e.value)) AS record
      FROM
        UNNEST(map) AS e
      GROUP BY
        e.key
    )

    SELECT
       ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(record.key, record.value))
    FROM
      summed_counts
  )
);

CREATE TEMP FUNCTION udf_buckets_to_map (buckets ARRAY<STRING>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Given an array of values, transform them into a histogram MAP
  -- with the number of each key in the `buckets` array
  (
    SELECT
       ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, 1.0))
    FROM
      UNNEST(buckets) AS bucket
  )
);

CREATE TEMP FUNCTION udf_fill_buckets(input_map ARRAY<STRUCT<key STRING, value FLOAT64>>, buckets ARRAY<STRING>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Given a MAP `input_map`, fill in any missing keys with value `0.0`
  (
    WITH total_counts AS (
      SELECT
        key,
        COALESCE(e.value, 0.0) AS value
      FROM
        UNNEST(buckets) as key
      LEFT JOIN
        UNNEST(input_map) AS e ON SAFE_CAST(key AS STRING) = e.key
    )
    
    SELECT
      ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(SAFE_CAST(key AS STRING), value))
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
    metric_type,
    key,
    agg_type,
    CASE
      WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
      THEN SAFE_CAST(udf_bucket(SAFE_CAST(agg_value AS FLOAT64), 0, 1000, 50, 'scalar') AS STRING)
      WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
      THEN agg_value
    END AS bucket
  FROM
    clients_aggregates_v1
),

normalized_histograms AS
  (SELECT
      client_id,
      os,
      app_version,
      app_build_id,
      channel,
      bucket_range,
      aggregate.metric as metric,
      aggregate.metric_type AS metric_type,
      aggregate.key AS key,
      aggregate.agg_type as agg_type,
      udf_normalized_sum(
        udf_aggregate_map_sum(ARRAY_AGG(STRUCT<key_value ARRAY<STRUCT <key STRING, value INT64>>>(aggregate.value)) OVER w1)) AS aggregates
    FROM
      clients_daily_histogram_aggregates_v1
    CROSS JOIN
      UNNEST(histogram_aggregates) AS aggregate
    WHERE ARRAY_LENGTH(value) > 0
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
                aggregate.agg_type)),

bucketed_histograms AS
  (SELECT
      client_id,
      os,
      app_version,
      app_build_id,
      channel,
      bucket_range,
      metric,
      metric_type,
      normalized_histograms.key AS key,
      agg_type,
      udf_bucket(SAFE_CAST(agg.key AS FLOAT64), bucket_range.first_bucket, bucket_range.last_bucket, bucket_range.num_buckets, metric_type) AS bucket,
      agg.value AS value
  FROM normalized_histograms
  CROSS JOIN UNNEST(aggregates) AS agg
  WHERE bucket_range.num_buckets > 0),

clients_aggregates AS
  (SELECT
    os,
    app_version,
    app_build_id,
    channel,
    CAST(bucket_range.first_bucket AS INT64) AS min_bucket,
    CAST(bucket_range.last_bucket AS INT64) AS max_bucket,
    CAST(bucket_range.num_buckets AS INT64) AS num_buckets,
    metric,
    metric_type,
    key,
    agg_type,
    bucket,
    STRUCT<key STRING, value FLOAT64>(
      CAST(bucket AS STRING),
      1.0 * SUM(value)
    ) AS record
  FROM bucketed_histograms
  GROUP BY
    os,
    app_version,
    app_build_id,
    channel,
    min_bucket,
    max_bucket,
    num_buckets,
    metric,
    metric_type,
    key,
    agg_type,
    bucket)

SELECT
  os,
  app_version,
  app_build_id,
  channel,
  clients_aggregates.metric,
  metric_type,
  key,
  agg_type,
  udf_fill_buckets(udf_dedupe_map_sum(
      ARRAY_AGG(record)
  ), udf_get_buckets(min_bucket, max_bucket, num_buckets, metric_type)) AS aggregates
FROM clients_aggregates
WHERE min_bucket IS NOT NULL
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  min_bucket,
  max_bucket,
  num_buckets

UNION ALL

SELECT
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_get_buckets(0, 1000, 50, metric_type)
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) as os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_get_buckets(0, 1000, 50, metric_type)
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_get_buckets(0, 1000, 50, metric_type)
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type

UNION ALL

SELECT
  os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_get_buckets(0, 1000, 50, metric_type)
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_get_buckets(0, 1000, 50, metric_type)
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_get_buckets(0, 1000, 50, metric_type)
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_get_buckets(0, 1000, 50, metric_type)
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_get_buckets(0, 1000, 50, metric_type)
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_get_buckets(0, 1000, 50, metric_type)
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_get_buckets(0, 1000, 50, metric_type)
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type
