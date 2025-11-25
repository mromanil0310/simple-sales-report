
  create or replace   view TECH_TEST.ANALYTICS.stg_orders
  
  
  
  
  as (
    

WITH src AS (
    SELECT
        ORDER_ID,
        USER_ID,
        AMOUNT::NUMBER(10,2) AS AMOUNT,
        ORDER_DATE::DATE AS ORDER_DATE
    FROM TECH_TEST.RAW_DATA.orders
)

SELECT * FROM src
  );

