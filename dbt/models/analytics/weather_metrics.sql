WITH daily AS (

    SELECT *
    FROM {{ ref('weather_daily') }}
    WHERE data_type = 'HISTORICAL'

),

metrics AS (

    SELECT

        latitude,
        longitude,
        weather_date,

        avg_temperature,
        max_temperature,
        min_temperature,
        avg_humidity,
        avg_apparent_temperature,

        daily_precipitation,
        daily_rain,
        daily_showers,
        daily_snowfall,

        avg_wind_speed,
        max_wind_speed,

        rainy_hours,
        dominant_weather_code,

        /* 7-day moving average temperature */
        AVG(avg_temperature) OVER (
            PARTITION BY latitude, longitude
            ORDER BY weather_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS temperature_7day_moving_avg,

        /* 7-day rolling rainfall */
        SUM(daily_precipitation) OVER (
            PARTITION BY latitude, longitude
            ORDER BY weather_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rainfall_7day_rolling,

        /* Historical temperature baseline */
        AVG(avg_temperature) OVER (
            PARTITION BY latitude, longitude
        ) AS historical_avg_temperature,

        /* Dry day */
        CASE
            WHEN daily_precipitation < 1
                THEN 1
            ELSE 0
        END AS is_dry_day

    FROM daily

)

SELECT

    *,

    /* Temperature anomaly */
    avg_temperature - historical_avg_temperature
        AS temperature_anomaly,

    /* Consecutive dry spell */
    SUM(
        CASE
            WHEN is_dry_day = 1 THEN 1
            ELSE 0
        END
    ) OVER (
        PARTITION BY latitude, longitude
        ORDER BY weather_date
        ROWS UNBOUNDED PRECEDING
    ) AS dry_spell_group

FROM metrics