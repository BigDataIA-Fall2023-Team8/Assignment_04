USE DATABASE A4_DB;
USE WAREHOUSE A4_WH;
USE SCHEMA STAGING;
CREATE OR REPLACE FUNCTION calculate_conversion_rate(estimated_views FLOAT, estimated_purchases FLOAT)
RETURNS FLOAT
AS
$$
    CASE
        WHEN estimated_views > 0 THEN (estimated_purchases / estimated_views) * 100
        ELSE 0.0
    END
$$;

