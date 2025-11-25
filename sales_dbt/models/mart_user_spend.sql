{{ config(
    materialized = 'table'
) }}

WITH USERS AS (
    SELECT
        USER_ID,
        NAME,
        JOINED_DATE
    FROM {{ ref('stg_users') }}
),
ORDERS AS (
    SELECT
        USER_ID,
        AMOUNT
    FROM {{ ref('stg_orders') }}
),
AGG_ORDERS AS (
    SELECT
        USER_ID,
        COUNT(*) AS NUMBER_OF_ORDERS,
        SUM(AMOUNT) AS TOTAL_SPEND
    FROM ORDERS
    GROUP BY USER_ID
)

SELECT
    U.USER_ID,
    U.NAME AS FIRST_NAME,
    U.JOINED_DATE,
    COALESCE(A.NUMBER_OF_ORDERS, 0) AS NUMBER_OF_ORDERS,
    COALESCE(A.TOTAL_SPEND, 0) AS TOTAL_SPEND
FROM USERS U
LEFT JOIN AGG_ORDERS A
    ON U.USER_ID = A.USER_ID
