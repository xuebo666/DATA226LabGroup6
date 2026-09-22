WITH daily AS (
    SELECT *
    FROM {{ ref('weather_daily') }}
    WHERE data_type = 'HISTORICAL'
),

metrics AS (
    SELECT
        latitude, longitude, weather_date,
        avg_temperature, max_temperature, min_temperature,
        avg_humidity, avg_apparent_temperature,
        daily_precipitation, daily_rain, daily_showers, daily_snowfall,
        avg_wind_speed, max_wind_speed,
        rainy_hours, dominant_weather_code,

        ROUND(AVG(avg_temperature) OVER (
            PARTITION BY latitude, longitude ORDER BY weather_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS temperature_7day_moving_avg,

        ROUND(SUM(daily_precipitation) OVER (
            PARTITION BY latitude, longitude ORDER BY weather_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS rainfall_7day_rolling,

        ROUND(AVG(avg_temperature) OVER (PARTITION BY latitude, longitude), 2) AS historical_avg_temperature,

        CASE WHEN daily_precipitation < 1 THEN 1 ELSE 0 END AS is_dry_day
    FROM daily
),

anomaly AS (
    SELECT *,
        ROUND(avg_temperature - historical_avg_temperature, 2) AS temperature_anomaly,
        SUM(CASE WHEN is_dry_day = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY latitude, longitude
            ORDER BY weather_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS dry_spell_group
    FROM metrics
)

SELECT
    * EXCLUDE (dry_spell_group),
    CASE
        WHEN is_dry_day = 1
        THEN SUM(is_dry_day) OVER (
            PARTITION BY latitude, longitude, dry_spell_group
            ORDER BY weather_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        ELSE 0
    END AS dry_spell_length
FROM anomaly