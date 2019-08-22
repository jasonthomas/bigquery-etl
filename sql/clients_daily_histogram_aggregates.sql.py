#!/usr/bin/env python3
"""clients_daily_histogram_aggregates query generator."""
import sys
import json
import argparse
import textwrap
import subprocess
import urllib.request


PROBE_INFO_SERVICE = (
    "https://probeinfo.telemetry.mozilla.org/firefox/all/main/all_probes"
)

p = argparse.ArgumentParser()
p.add_argument(
    "--agg-type",
    type=str,
    help="One of scalars/histograms/booleans/events",
    required=True,
)


def generate_sql(opts, additional_queries, windowed_clause, select_clause):
    """Create a SQL query for the clients_daily_histogram_aggregates dataset."""
    get_keyval_pairs = """
        CREATE TEMPORARY FUNCTION get_keyval_pairs(y STRING)
        RETURNS ARRAY<STRING>
        LANGUAGE js AS
        '''
          var z = new Array();
          node = JSON.parse(y);
          Object.keys(node).map(function(key) {
            value = node[key].toString();
            z.push(key + ":" + value);
          });
          return z
        ''';
    """

    string_to_arr = """
        -- convert a string like '[5, 6, 7]' to an array struct
        CREATE TEMPORARY FUNCTION string_to_arr(y STRING)
        RETURNS ARRAY<STRING>
        LANGUAGE js AS
        '''
          return JSON.parse(y);
        ''';
    """

    """Create a SQL query for the clients_daily_histogram_aggregates dataset."""
    return textwrap.dedent(
        f"""-- Query generated by: sql/clients_daily_histogram_aggregates.sql.py
        {get_keyval_pairs}

        {string_to_arr}

        CREATE TEMP FUNCTION udf_get_bucket_range(histograms ARRAY<STRING>) AS ((
          WITH buckets AS (
            SELECT
              string_to_arr(JSON_EXTRACT(histogram, "$.range")) AS bucket_range,
              CAST(JSON_EXTRACT(histogram, "$.bucket_count") AS INT64) AS num_buckets
            FROM UNNEST(histograms) AS histogram
            WHERE histogram IS NOT NULL
            LIMIT 1
          )

          SELECT AS STRUCT
            CAST(bucket_range[OFFSET(0)] AS INT64) AS first_bucket,
            CAST(bucket_range[OFFSET(1)] AS INT64) AS last_bucket,
            num_buckets
          FROM
            buckets));


        CREATE TEMP FUNCTION udf_get_histogram_type(histograms ARRAY<STRING>) AS ((
            SELECT
              CASE CAST(JSON_EXTRACT(histogram, "$.histogram_type") AS INT64)
                WHEN 0 THEN 'histogram-exponential'
                WHEN 1 THEN 'histogram-linear'
                WHEN 2 THEN 'histogram-boolean'
                WHEN 3 THEN 'histogram-flag'
                WHEN 4 THEN 'histogram-count'
                WHEN 5 THEN 'histogram-categorical'
              END AS histogram_type
            FROM UNNEST(histograms) AS histogram
            WHERE histogram IS NOT NULL
            LIMIT 1
        ));

        CREATE TEMP FUNCTION
          udf_aggregate_json_sum(histograms ARRAY<STRING>) AS (ARRAY(
              SELECT
                AS STRUCT SPLIT(keyval, ':')[OFFSET(0)] AS key,
                SUM(CAST(SPLIT(keyval, ':')[OFFSET(1)] AS INT64)) AS value
              FROM
                UNNEST(histograms) AS histogram,
                UNNEST(get_keyval_pairs(JSON_EXTRACT(histogram, "$.values"))) AS keyval
              WHERE histogram IS NOT NULL
              GROUP BY key));

        WITH
          -- normalize client_id and rank by document_id
          numbered_duplicates AS (
            SELECT
                ROW_NUMBER() OVER (
                    PARTITION BY
                        client_id,
                        submission_timestamp,
                        document_id
                    ORDER BY submission_timestamp
                    ASC
                ) AS _n,
                * REPLACE(LOWER(client_id) AS client_id)
            FROM `moz-fx-data-shar-nonprod-efed.telemetry_live.main_v4`
            WHERE DATE(submission_timestamp) = '2019-07-17'
            AND application.channel in (
                "release", "esr", "beta", "aurora", "default", "nightly"
            )
            AND client_id IS NOT NULL
          ),


        -- Deduplicating on document_id is necessary to get valid SUM values.
        deduplicated AS (
            SELECT * EXCEPT (_n)
            FROM numbered_duplicates
            WHERE _n = 1
        ),

        {additional_queries}

        -- Aggregate by client_id using windows
        windowed AS (
            {windowed_clause}
        )
        {select_clause}
        """
    )


def _get_keyed_histogram_sql(probes):
    probes_struct = []
    for probe in probes:
        probes_struct.append(f"('{probe}', payload.keyed_histograms.{probe})")

    probes_arr = ",\n\t\t\t".join(probes_struct)

    probes_string = """
        metric,
        key,
        ARRAY_AGG(value) OVER w1 as bucket_range,
        ARRAY_AGG(value) OVER w1 as value
    """

    additional_queries = f"""
        grouped_metrics AS
          (select
            submission_timestamp,
            DATE(submission_timestamp) as submission_date,
            client_id,
            normalized_os as os,
            SPLIT(application.version, '.')[OFFSET(0)] AS app_version,
            application.build_id AS app_build_id,
            normalized_channel AS channel,
            ARRAY<STRUCT<
                name STRING,
                value ARRAY<STRUCT<key STRING, value STRING>>
            >>[
              {probes_arr}
            ] as metrics
          FROM deduplicated),

          flattened_metrics AS
            (SELECT
              submission_timestamp,
              submission_date,
              client_id,
              os,
              app_version,
              app_build_id,
              channel,
              metrics.name AS metric,
              value.key AS key,
              value.value AS value
            FROM grouped_metrics
            CROSS JOIN UNNEST(metrics) AS metrics
            CROSS JOIN unnest(metrics.value) AS value),
    """

    windowed_clause = f"""
        SELECT
            ROW_NUMBER() OVER w1_unframed AS _n,
            submission_date,
            client_id,
            os,
            app_version,
            app_build_id,
            channel,
            {probes_string}
            FROM flattened_metrics
            WINDOW
                -- Aggregations require a framed window
                w1 AS (
                    PARTITION BY
                        client_id,
                        submission_date,
                        os,
                        app_version,
                        app_build_id,
                        channel,
                        metric,
                        key
                    ORDER BY `submission_timestamp` ASC ROWS BETWEEN UNBOUNDED PRECEDING
                    AND UNBOUNDED FOLLOWING
                ),

                -- ROW_NUMBER does not work on a framed window
                w1_unframed AS (
                    PARTITION BY
                        client_id,
                        submission_date,
                        os,
                        app_version,
                        app_build_id,
                        channel,
                        metric,
                        key
                    ORDER BY `submission_timestamp` ASC)
    """

    select_clause = """
        select
            client_id,
            submission_date,
            os,
            app_version,
            app_build_id,
            channel,
            ARRAY_AGG(STRUCT<
                metric STRING,
                metric_type STRING,
                key STRING,
                agg_type STRING,
                bucket_range STRUCT<
                    first_bucket INT64,
                    last_bucket INT64,
                    num_buckets INT64
                >,
                value ARRAY<STRUCT<key STRING, value INT64>>
            >(
                metric,
                udf_get_histogram_type(bucket_range),
                key,
                '',
                udf_get_bucket_range(bucket_range),
                udf_aggregate_json_sum(value)
            )) AS histogram_aggregates
        from windowed
        where _n = 1
        group by 1,2,3,4,5,6
    """

    return {
        "additional_queries": additional_queries,
        "select_clause": select_clause,
        "windowed_clause": windowed_clause,
    }


def get_histogram_probes_sql_strings(probes, histogram_type):
    """Put together the subsets of SQL required to query histograms."""
    sql_strings = {}
    if histogram_type == "keyed_histograms":
        return _get_keyed_histogram_sql(probes)

    probe_structs = []
    for probe in probes:
        agg_string = (
            "('{metric}', "
            "'histogram', '', "
            "'summed-histogram', "
            "ARRAY_AGG(payload.histograms.{metric}) OVER w1, "
            "ARRAY_AGG(payload.histograms.{metric}) OVER w1)"
        )
        probe_structs.append(agg_string.format(metric=probe))

    probes_arr = ",\n\t\t\t".join(probe_structs)
    probes_string = f"""
            ARRAY<STRUCT<
                metric STRING,
                metric_type STRING,
                key STRING,
                agg_type STRING,
                bucket_range ARRAY<STRING>,
                value ARRAY<STRING>
            >> [
            {probes_arr}
        ] AS histogram_aggregates
    """

    sql_strings[
        "select_clause"
    ] = f"""
        SELECT
            * EXCEPT (_n) REPLACE (
                ARRAY(
                SELECT AS STRUCT
                    * REPLACE (
                      udf_aggregate_json_sum(value) AS value,
                      udf_get_bucket_range(bucket_range) AS bucket_range,
                      udf_get_histogram_type(bucket_range) AS metric_type
                    )
                FROM
                    UNNEST(histogram_aggregates)
                ) AS histogram_aggregates
            )
        FROM
            windowed
        WHERE
            _n = 1
    """

    sql_strings[
        "windowed_clause"
    ] = f"""
        SELECT
            ROW_NUMBER() OVER w1_unframed AS _n,
            DATE(submission_timestamp) as submission_date,
            client_id,
            normalized_os as os,
            SPLIT(application.version, '.')[OFFSET(0)] AS app_version,
            application.build_id AS app_build_id,
            normalized_channel AS channel,
                {probes_string}
            FROM deduplicated
            WINDOW
                -- Aggregations require a framed window
                w1 AS (
                    PARTITION BY
                        client_id,
                        DATE(submission_timestamp),
                        normalized_os,
                        SPLIT(application.version, '.')[OFFSET(0)],
                        application.build_id,
                        normalized_channel
                    ORDER BY `submission_timestamp` ASC ROWS BETWEEN UNBOUNDED PRECEDING
                    AND UNBOUNDED FOLLOWING
                ),

                -- ROW_NUMBER does not work on a framed window
                w1_unframed AS (
                    PARTITION BY
                        client_id,
                        DATE(submission_timestamp),
                        normalized_os,
                        SPLIT(application.version, '.')[OFFSET(0)],
                        application.build_id,
                        normalized_channel
                    ORDER BY `submission_timestamp` ASC
                )
    """

    return sql_strings


def get_histogram_probes(histogram_type):
    """Return relevant histogram probes."""
    project = "moz-fx-data-shar-nonprod-efed"
    main_summary_histograms = set()
    process = subprocess.Popen(
        [
            "bq",
            "show",
            "--schema",
            "--format=json",
            f"{project}:telemetry_live.main_v4",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    if process.returncode > 0:
        raise Exception(
            f"Call to bq exited non-zero: {process.returncode}", stdout, stderr
        )
    main_summary_schema = json.loads(stdout)

    # Fetch the histograms field
    histograms_field = None
    for field in main_summary_schema:
        if field["name"] != "payload":
            continue

        for payload_field in field["fields"]:
            if payload_field["name"] == histogram_type:
                histograms_field = payload_field
                break

    if histograms_field is None:
        return

    for histogram in histograms_field.get("fields", []):
        main_summary_histograms.add(histogram["name"])

    with urllib.request.urlopen(PROBE_INFO_SERVICE) as url:
        data = json.loads(url.read().decode())
        histogram_probes = set(
            [
                x.replace("histogram/", "").replace(".", "_").lower()
                for x in data.keys()
                if x.startswith("histogram/")
            ]
        )
        relevant_probes = histogram_probes.intersection(main_summary_histograms)
        return relevant_probes


def main(argv, out=print):
    """Print a clients_daily_histogram_aggregates query to stdout."""
    opts = vars(p.parse_args(argv[1:]))
    sql_string = ""

    if opts["agg_type"] in ("histograms", "keyed_histograms"):
        histogram_probes = get_histogram_probes(opts["agg_type"])
        sql_string = get_histogram_probes_sql_strings(
            histogram_probes, opts["agg_type"]
        )
    else:
        raise ValueError("agg-type must be one of histograms, keyed_histograms")

    out(
        generate_sql(
            opts,
            sql_string.get("additional_queries", ""),
            sql_string["windowed_clause"],
            sql_string["select_clause"],
        )
    )


if __name__ == "__main__":
    main(sys.argv)
