#!/usr/bin/env python3
"""clients_daily_scalar_aggregates query generator."""
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


def generate_sql(
    opts,
    aggregates,
    additional_queries,
    additional_partitions,
    select_clause,
    querying_table,
):
    """Create a SQL query for the clients_daily_scalar_aggregates dataset."""
    return textwrap.dedent(
        f"""-- Query generated by: sql/clients_daily_scalar_aggregates.sql.py
        CREATE TEMP FUNCTION
          udf_aggregate_map_sum(maps ANY TYPE) AS (STRUCT(ARRAY(
              SELECT
                AS STRUCT key,
                SUM(value) AS value
              FROM
                UNNEST(maps),
                UNNEST(key_value)
              GROUP BY
        key) AS key_value));

        CREATE TEMP FUNCTION
          udf_aggregate_keyed_map_sum(maps ANY TYPE) AS (STRUCT(ARRAY(
              SELECT
                AS STRUCT key,
                udf_aggregate_map_sum(ARRAY_AGG(value)) AS value
              FROM
                UNNEST(maps),
                UNNEST(key_value)
              GROUP BY
        key) AS key_value));

        WITH
            -- normalize client_id and rank by document_id
            numbered_duplicates AS (
                SELECT
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            client_id,
                            submission_date_s3,
                            document_id
                        ORDER BY `timestamp`
                        ASC
                    ) AS _n,
                    * REPLACE(LOWER(client_id) AS client_id)
                FROM main_summary_v4
                WHERE submission_date_s3 = @submission_date
                AND channel in ("release", "esr", "beta", "aurora", "default", "nightly")
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
                SELECT
                    ROW_NUMBER() OVER w1_unframed AS _n,
                    submission_date_s3 as submission_date,
                    client_id,
                    os,
                    SPLIT(app_version, '.')[OFFSET(0)] AS app_version,
                    app_build_id,
                    channel,
                    {aggregates}
                FROM {querying_table}
                WINDOW
                    -- Aggregations require a framed window
                    w1 AS (
                        PARTITION BY
                            client_id,
                            submission_date_s3,
                            os,
                            app_version,
                            app_build_id,
                            channel
                            {additional_partitions}
                        ORDER BY `timestamp` ASC ROWS BETWEEN UNBOUNDED PRECEDING
                        AND UNBOUNDED FOLLOWING
                    ),

                    -- ROW_NUMBER does not work on a framed window
                    w1_unframed AS (
                        PARTITION BY
                            client_id,
                            submission_date_s3,
                            os,
                            app_version,
                            app_build_id,
                            channel
                            {additional_partitions}
                        ORDER BY `timestamp` ASC
                    )
            )
            {select_clause}
        """
    )


def get_histogram_probes_sql_strings(probes, histogram_type):
    """Put together the subsets of SQL required to query histograms."""
    probe_structs = []
    for probe in probes:
        probe_structs.append(
            "('{metric}', 'summed-histogram', ARRAY_AGG({metric}) OVER w1)".format(
                metric=probe
            )
        )

    probes_arr = ",\n\t\t\t".join(probe_structs)
    value_arr = "value ARRAY<STRUCT<key_value ARRAY<STRUCT<key INT64, value INT64>>>>"
    if histogram_type == "string-histogram":
        value_arr = (
            "value ARRAY<STRUCT<key_value ARRAY<STRUCT<key STRING, value INT64>>>>"
        )
    elif histogram_type == "keyed-histogram":
        value_arr = (
            "value ARRAY<STRUCT<key_value ARRAY<STRUCT<key STRING, "
            "value STRUCT<key_value ARRAY<STRUCT<key INT64, value INT64>>>>>>>"
        )

    probes_string = f"""
            ARRAY<STRUCT<
                metric STRING,
                agg_type STRING,
                {value_arr}
            >> [
            {probes_arr}
        ] AS histogram_aggregates
    """

    agg_function = (
        "udf_aggregate_keyed_map_sum"
        if histogram_type == "keyed-histogram"
        else "udf_aggregate_map_sum"
    )
    select_clause = f"""
        SELECT
            * EXCEPT (_n) REPLACE (
                ARRAY(
                SELECT AS STRUCT
                    * REPLACE (
                        CASE
                        WHEN agg_type = 'summed-histogram' THEN
                            {agg_function}(value)
                        ELSE
                            error(CONCAT('Unhandled agg_type: ', agg_type))
                        END AS value
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

    return {"probes_string": probes_string, "select_clause": select_clause}


def get_histogram_type(keyval_fields):
    """Return the type of histogram given its schema."""
    if (
        len(keyval_fields) == 2
        and keyval_fields[0].get("type", None) == "INTEGER"
        and keyval_fields[1].get("type", None) == "INTEGER"
    ):
        return "histogram"

    if (
        len(keyval_fields) == 2
        and keyval_fields[0].get("type", None) == "STRING"
        and keyval_fields[1].get("type", None) == "INTEGER"
    ):
        return "string-histogram"

    if (
        len(keyval_fields) == 2
        and keyval_fields[0].get("type", None) == "STRING"
        and keyval_fields[1].get("type", None) == "RECORD"
        and keyval_fields[1]["fields"][0]["fields"][0]["type"] == "INTEGER"
    ):
        return "keyed-histogram"


def get_histogram_probes(histogram_type):
    """
    Return relevant histogram probes.

    Keep track of probe names before they're stripped of "histogram_parent_"
    and"histogram_content_" prefixes so they can be used in the query.
    """
    main_summary_original_probes = {}
    project = "moz-fx-data-derived-datasets"
    main_summary_histograms = set()
    process = subprocess.Popen(
        [
            "bq",
            "show",
            "--schema",
            "--format=json",
            f"{project}:telemetry.main_summary_v4",
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

    for field in main_summary_schema:
        if not field["name"].startswith("histogram"):
            continue

        subfields = field.get("fields", [])
        keyval_fields = (
            subfields[0].get("fields", [])
            if isinstance(subfields, list) and len(subfields) > 0
            else []
        )

        if histogram_type != get_histogram_type(keyval_fields):
            continue

        field_name = (
            field["name"]
            .replace("histogram_content_", "")
            .replace("histogram_parent_", "")
        )
        main_summary_histograms.add(field_name)
        main_summary_original_probes[field_name] = field["name"]

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
        return set([main_summary_original_probes[probe] for probe in relevant_probes])


def get_keyed_scalar_probes_sql_string(probes):
    """Put together the subsets of SQL required to query keyed scalars."""
    probes_struct = []
    for probe in probes:
        probes_struct.append(f"('{probe}', {probe})")

    probes_arr = ",\n\t\t\t".join(probes_struct)

    additional_queries = f"""
        grouped_metrics AS
          (select
            timestamp,
            client_id,
            submission_date_s3,
            os,
            app_version,
            app_build_id,
            channel,
            ARRAY<STRUCT<
                name STRING,
                value STRUCT<key_value ARRAY<STRUCT<key STRING, value INT64>>>
            >>[
              {probes_arr}
            ] as metrics
          FROM deduplicated),

          flattened_metrics AS
            (SELECT
              timestamp,
              client_id,
              submission_date_s3,
              os,
              app_version,
              app_build_id,
              channel,
              metrics.name AS metric,
              value.key AS key,
              value.value AS value
            FROM grouped_metrics
            CROSS JOIN unnest(metrics) AS metrics,
            unnest(metrics.value.key_value) AS value),
    """

    probes_string = """
                metric,
                key,
                MAX(value) OVER w1 as max,
                MIN(value) OVER w1 as min,
                AVG(value) OVER w1 as avg,
                SUM(value) OVER w1 as sum
    """

    select_clause = """
        select
              client_id,
              submission_date,
              os,
              app_version,
              app_build_id,
              channel,
              ARRAY_CONCAT_AGG(
                [(metric, key, 'max', max),
                (metric, key, 'min', min),
                (metric, key,  'avg', avg),
                (metric, key, 'sum', sum)
              ]) AS scalar_aggregates
        from windowed
        where _n = 1
        group by 1,2,3,4,5,6
    """

    querying_table = "flattened_metrics"

    additional_partitions = """,
                            metric,
                            key
    """

    return {
        "probes_string": probes_string,
        "additional_queries": additional_queries,
        "additional_partitions": additional_partitions,
        "select_clause": select_clause,
        "querying_table": querying_table,
    }


def get_scalar_probes_sql_strings(probes, scalar_type):
    """Put together the subsets of SQL required to query scalars or booleans."""
    if scalar_type == "keyed-scalar":
        return get_keyed_scalar_probes_sql_string(probes["keyed"])

    probe_structs = []
    for probe in probes["scalars"]:
        probe_structs.append(
            f"('{probe}', 'max', max(CAST({probe} AS INT64)) OVER w1, 0, 1000, 50)"
        )
        probe_structs.append(
            f"('{probe}', 'avg', avg(CAST({probe} AS INT64)) OVER w1, 0, 1000, 50)"
        )
        probe_structs.append(
            f"('{probe}', 'min', min(CAST({probe} AS INT64)) OVER w1, 0, 1000, 50)"
        )
        probe_structs.append(
            f"('{probe}', 'sum', sum(CAST({probe} AS INT64)) OVER w1, 0, 1000, 50)"
        )

    for probe in probes["booleans"]:
        probe_structs.append(
            (
                f"('{probe}', 'false', sum(case when {probe} = False "
                "then 1 else 0 end) OVER w1, 0, 2, 2)"
            )
        )
        probe_structs.append(
            (
                f"('{probe}', 'true', sum(case when {probe} = True then 1 else 0 end) "
                "OVER w1, 0, 2, 2)"
            )
        )

    probes_arr = ",\n\t\t\t".join(probe_structs)
    probes_string = f"""
            ARRAY<STRUCT<
                metric STRING,
                agg_type STRING,
                value FLOAT64,
                min_bucket INT64,
                max_bucket INT64,
                num_buckets INT64
            >> [
                {probes_arr}
            ] AS scalar_aggregates
    """

    select_clause = f"""
        SELECT
            * EXCEPT(_n)
        FROM
            windowed
        WHERE
            _n = 1
    """

    return {"probes_string": probes_string, "select_clause": select_clause}


def get_scalar_probes():
    """Find all scalar probes in main summary.

    Note: that non-integer scalar probes are not included.
    """
    project = "moz-fx-data-derived-datasets"
    main_summary_scalars = set()
    main_summary_record_scalars = set()
    main_summary_boolean_record_scalars = set()
    main_summary_boolean_scalars = set()

    process = subprocess.Popen(
        [
            "bq",
            "show",
            "--schema",
            "--format=json",
            f"{project}:telemetry.main_summary_v4",
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

    for field in main_summary_schema:
        if field["name"].startswith("scalar_parent") and field["type"] == "INTEGER":
            main_summary_scalars.add(field["name"])
        elif field["name"].startswith("scalar_parent") and field["type"] == "BOOLEAN":
            main_summary_boolean_scalars.add(field["name"])
        elif field["name"].startswith("scalar_parent") and field["type"] == "RECORD":
            if field["fields"][0]["fields"][1]["type"] == "BOOLEAN":
                main_summary_boolean_record_scalars.add(field["name"])
            else:
                main_summary_record_scalars.add(field["name"])

    # Find the intersection between relevant scalar probes
    # and those that exist in main summary
    with urllib.request.urlopen(PROBE_INFO_SERVICE) as url:
        data = json.loads(url.read().decode())
        scalar_probes = set(
            [
                x.replace("scalar/", "scalar_parent_").replace(".", "_")
                for x in data.keys()
                if x.startswith("scalar/")
            ]
        )
        return {
            "scalars": scalar_probes.intersection(main_summary_scalars),
            "booleans": scalar_probes.intersection(main_summary_boolean_scalars),
            "keyed": scalar_probes.intersection(main_summary_record_scalars),
        }


def main(argv, out=print):
    """Print a clients_daily_scalar_aggregates query to stdout."""
    opts = vars(p.parse_args(argv[1:]))
    sql_string = ""

    if opts["agg_type"] in ("scalar", "keyed-scalar"):
        scalar_probes = get_scalar_probes()
        sql_string = get_scalar_probes_sql_strings(scalar_probes, opts["agg_type"])
    elif opts["agg_type"] in ("histogram", "keyed-histogram", "string-histogram"):
        histogram_probes = get_histogram_probes(opts["agg_type"])
        sql_string = get_histogram_probes_sql_strings(
            histogram_probes, opts["agg_type"]
        )
    else:
        raise ValueError("agg-type must be one of scalar/histogram")

    out(
        generate_sql(
            opts,
            sql_string["probes_string"],
            sql_string.get("additional_queries", ""),
            sql_string.get("additional_partitions", ""),
            sql_string["select_clause"],
            sql_string.get("querying_table", "deduplicated"),
        )
    )


if __name__ == "__main__":
    main(sys.argv)
