-- models/my_model.sql

-- models/my_first_dbt_model.sql

WITH source_data AS (
    SELECT 
        channel,
        message_id,
        date,
        text,
        media
    FROM 
        {{ ref('raw_data') }}  
)

SELECT 
    ROW_NUMBER() OVER () AS id, 
    channel,
    message_id,
    date,
    text,
    media,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    LENGTH(text) AS text_length
FROM 
    source_data
WHERE 
    message_id IS NOT NULL

