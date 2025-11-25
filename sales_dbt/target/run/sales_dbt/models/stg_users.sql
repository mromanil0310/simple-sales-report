
  create or replace   view TECH_TEST.ANALYTICS.stg_users
  
  
  
  
  as (
    

WITH src AS (
    SELECT
        USER_ID,
        FIRST_NAME AS NAME,
        JOINED_AT::DATE AS JOINED_DATE
    FROM TECH_TEST.RAW_DATA.users
)
SELECT * FROM src
  );

