{{ config(materialized='view') }}

WITH src AS (
    SELECT
        ORDER_ID,
        USER_ID,
        AMOUNT::NUMBER(10,2) AS AMOUNT,
        ORDER_DATE::DATE AS ORDER_DATE
    FROM {{ source('raw', 'orders') }}
)

SELECT * FROM src
