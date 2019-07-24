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
