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
            WHEN weather_time < CURRENT_TIMESTAMP()
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

        ROUND(AVG(temperature), 2) AS avg_temperature,
        ROUND(MAX(temperature), 2) AS max_temperature,
        ROUND(MIN(temperature), 2) AS min_temperature,

        ROUND(AVG(humidity), 2) AS avg_humidity,
        ROUND(AVG(apparent_temperature), 2) AS avg_apparent_temperature,

        ROUND(SUM(precipitation), 2) AS daily_precipitation,
        ROUND(SUM(rain), 2) AS daily_rain,
        ROUND(SUM(showers), 2) AS daily_showers,
        ROUND(SUM(snowfall), 2) AS daily_snowfall,

        ROUND(AVG(wind_speed), 2) AS avg_wind_speed,
        ROUND(MAX(wind_speed), 2) AS max_wind_speed,

        COUNT_IF(precipitation >= 1) AS rainy_hours,
        MAX(CASE WHEN precipitation < 1 THEN 1 ELSE 0 END) AS has_dry_hour,
        MODE(weather_code) AS dominant_weather_code,
        data_type

    FROM hourly
    GROUP BY latitude, longitude, weather_date, data_type
) 

SELECT *
FROM daily