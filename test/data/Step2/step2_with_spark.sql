SELECT
    device_id,
    temperature,
    CAST(split(location_coords, ',')[1] AS DOUBLE) AS longitude,
    CAST(split(location_coords, ',')[0] AS DOUBLE) AS latitude,
    measured_at
FROM
    source_data