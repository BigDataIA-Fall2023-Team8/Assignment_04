/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       03_load_weather.sql
Author:       Jeremiah Hansen
Last Updated: 1/9/2023
-----------------------------------------------------------------------------*/

-- SNOWFLAKE ADVANTAGE: Data sharing/marketplace (instead of ETL)
-- SNOWFLAKE ADVANTAGE: Visual Studio Code Snowflake native extension (PrPr, Git integration)


USE ROLE A4_ROLE;
USE WAREHOUSE A4_WH;


-- ----------------------------------------------------------------------------
-- Step #1: Connect to weather data in Marketplace
-- ----------------------------------------------------------------------------

/*---
But what about data that needs constant updating - like the WEATHER data? We would
need to build a pipeline process to constantly update that data to keep it fresh.

Perhaps a better way to get this external data would be to source it from a trusted
data supplier. Let them manage the data, keeping it accurate and up to date.

Enter the Snowflake Data Cloud...

Weather Source is a leading provider of global weather and climate data and their
OnPoint Product Suite provides businesses with the necessary weather and climate data
to quickly generate meaningful and actionable insights for a wide range of use cases
across industries. Let's connect to the "Weather Source LLC: frostbyte" feed from
Weather Source in the Snowflake Data Marketplace by following these steps:

    -> Snowsight Home Button
         -> Marketplace
             -> Search: "Weather Source LLC: frostbyte" (and click on tile in results)
                 -> Click the blue "Get" button
                     -> Under "Options", adjust the Database name to read "FROSTBYTE_WEATHERSOURCE" (all capital letters)
                        -> Grant to "HOL_ROLE"
    
That's it... we don't have to do anything from here to keep this data updated.
The provider will do that for us and data sharing means we are always seeing
whatever they they have published.


-- You can also do it via code if you know the account/share details...
SET WEATHERSOURCE_ACCT_NAME = '*** PUT ACCOUNT NAME HERE AS PART OF DEMO SETUP ***';
SET WEATHERSOURCE_SHARE_NAME = '*** PUT ACCOUNT SHARE HERE AS PART OF DEMO SETUP ***';
SET WEATHERSOURCE_SHARE = $WEATHERSOURCE_ACCT_NAME || '.' || $WEATHERSOURCE_SHARE_NAME;

CREATE OR REPLACE DATABASE FROSTBYTE_WEATHERSOURCE
  FROM SHARE IDENTIFIER($WEATHERSOURCE_SHARE);

GRANT IMPORTED PRIVILEGES ON DATABASE FROSTBYTE_WEATHERSOURCE TO ROLE HOL_ROLE;
---*/


-- Let's look at the data - same 3-part naming convention as any other table

-- SELECT * FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES LIMIT 100;

-- SELECT * FROM AMAZON_SALES_AND_MARKET_SHARE_DEMO.SALES_ESTIMATES_SCHEMA.SALES_ESTIMATES_WEEKLY LIMIT 100;

-- SELECT * FROM CALENDAR_DATA_WITH_DATE_DIMENSIONS__FREE_READY_TO_USE.PUBLIC.CALENDAR_DATA LIMIT 100;

-- SELECT *
-- FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES
-- WHERE BRAND LIKE 'Various Artists & Antonio Vivaldi & Arcangelo%';


SELECT bd.brand_name AS BRAND

FROM A4_DB.PRODUCT_VIEWS_AND_PURCHASES_DIM_MODEL.BRAND_DIMENSION bd

JOIN A4_DB.PRODUCT_VIEWS_AND_PURCHASES_DIM_MODEL.fact_sales fs ON bd.brand_id = fs.brand_id

JOIN A4_DB.PRODUCT_VIEWS_AND_PURCHASES_DIM_MODEL.product_dimension pd ON fs.product_id = pd.product_id

JOIN A4_DB.PRODUCT_VIEWS_AND_PURCHASES_DIM_MODEL.site_dimension sd ON fs.site_id = sd.site_id

-- WHERE pd.main_category = 'Sportswear'

WHERE sd.site_name = 'amazon.com'

ORDER BY fs.estimated_views DESC

LIMIT 5;

