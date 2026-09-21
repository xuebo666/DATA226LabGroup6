WITH hourly AS (
    SELECT
        latitude,
        longitude,
        CAST(weather_time AS DATE) AS weather_date,

        temperature,
        humidity,
        apparent_temperature,
        precipitation,
        rain,
        showers,
        snowfall,
        weather_code,
        wind_speed,
        wind_direction,

        CASE
            WHEN weather_time <= CURRENT_TIMESTAMP()
                THEN 'HISTORICAL'
            ELSE 'FORECAST'
        END AS data_type

    FROM raw.weather_hourly
),

daily AS (
    SELECT
        latitude,
        longitude,
        weather_date,

        AVG(temperature) AS avg_temperature,
        MAX(temperature) AS max_temperature,
        MIN(temperature) AS min_temperature,

        AVG(humidity) AS avg_humidity,
        AVG(apparent_temperature) AS avg_apparent_temperature,

        SUM(precipitation) AS daily_precipitation,
        SUM(rain) AS daily_rain,
        SUM(showers) AS daily_showers,
        SUM(snowfall) AS daily_snowfall,

        AVG(wind_speed) AS avg_wind_speed,
        MAX(wind_speed) AS max_wind_speed,

        COUNT_IF(precipitation >= 1) AS rainy_hours,

        MAX(
            CASE
                WHEN precipitation < 1 THEN 1
                ELSE 0
            END
        ) AS has_dry_period,

        MODE(weather_code) AS dominant_weather_code,

        data_type

    FROM hourly

    GROUP BY
        latitude,
        longitude,
        weather_date,
        data_type
)

SELECT *
FROM daily