{{ config(materialized='view') }}

WITH src AS (
    SELECT
        USER_ID,
        FIRST_NAME AS NAME,
        JOINED_AT::DATE AS JOINED_DATE
    FROM {{ source('raw', 'users') }}
)
SELECT * FROM src
