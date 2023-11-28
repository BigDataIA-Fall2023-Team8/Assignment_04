USE DATABASE A4_DB;
USE WAREHOUSE A4_WH;
USE SCHEMA STAGING;

CREATE OR REPLACE FUNCTION month_to_season(month_number INT)
RETURNS STRING
AS
$$
    CASE
        WHEN month_number IN (3, 4, 5) THEN 'Spring'
        WHEN month_number IN (6, 7, 8) THEN 'Summer'
        WHEN month_number IN (9, 10, 11) THEN 'Fall'
        ELSE 'Winter'
    END
$$;
