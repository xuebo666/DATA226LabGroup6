{% snapshot weather_hourly_snapshot %}

{{
    config(
        target_schema='ANALYTICS',
        unique_key='weather_key',
        strategy='check',
        check_cols=[
            'temperature',
            'humidity',
            'apparent_temperature',
            'precipitation',
            'rain',
            'showers',
            'snowfall',
            'weather_code',
            'wind_speed',
            'wind_direction'
        ]
    )
}}

SELECT
    CONCAT(
        latitude,
        '_',
        longitude,
        '_',
        TO_VARCHAR(weather_time)
    ) AS weather_key,

    latitude,
    longitude,
    weather_time,
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
    ingested_at

FROM raw.weather_hourly

{% endsnapshot %}