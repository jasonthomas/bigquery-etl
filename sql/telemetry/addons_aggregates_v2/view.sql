CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.addons_aggregates_v2` AS
SELECT
  submission_date AS submission_date_s3,
  *
FROM
  `moz-fx-data-derived-datasets.telemetry_derived.addons_aggregates_v2`
