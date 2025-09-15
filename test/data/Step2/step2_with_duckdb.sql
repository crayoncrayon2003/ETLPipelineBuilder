SELECT
    device_id,
    temperature,
    CAST(SPLIT_PART(location_coords, ',', 2) AS DOUBLE) AS longitude,
    CAST(SPLIT_PART(location_coords, ',', 1) AS DOUBLE) AS latitude,
    measured_at
FROM
    source_data;