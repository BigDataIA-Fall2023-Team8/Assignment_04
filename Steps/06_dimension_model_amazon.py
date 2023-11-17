# Function to create dimension tables
import os

# Load environment variables
snowflake_user = os.getenv('SNOWFLAKE_USER')
snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
snowflake_database = os.getenv('SNOWFLAKE_DATABASE')
snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')
openai_api_key = os.getenv('OPENAI_API_KEY')


def create_schema(session, schema_name):
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}").collect()
    return f"Schema '{schema_name}' has been created."

def create_and_populate_dimension_tables(session, schema_name):
    
    # Create the Brand dimension table
    session.sql(f"""DROP TABLE IF EXISTS {schema_name}.brand_dimension;""").collect()
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.brand_dimension (
            Brand_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Brand_Name VARCHAR(16777215) UNIQUE
        )
    """).collect()
    session.sql(f"""
        INSERT INTO {schema_name}.brand_dimension (Brand_Name)
        SELECT DISTINCT LEFT(pvp.BRAND, 16777215)
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES AS pvp
        FULL OUTER JOIN 
        AMAZON_SALES_AND_MARKET_SHARE_DEMO.SALES_ESTIMATES_SCHEMA.SALES_ESTIMATES_WEEKLY AS sew
        ON pvp.BRAND = sew.BRAND
        WHERE pvp.BRAND IS NOT NULL;
    """).collect()

# ----------------------------------------------------------------------------------------------------------------
    # Create and populate the Country dimension table
    session.sql(f"""DROP TABLE IF EXISTS {schema_name}.country_dimension;""").collect()
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.country_dimension (
            Country_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Country_Number INTEGER UNIQUE
        )
    """).collect()
    session.sql(f"""
        INSERT INTO {schema_name}.country_dimension (Country_Number)
        SELECT DISTINCT COUNTRY
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES
        WHERE COUNTRY IS NOT NULL
    """).collect()

# ----------------------------------------------------------------------------------------------------------------

    # Create and populate the Date dimension table
    session.sql(f"""DROP TABLE IF EXISTS {schema_name}.period_dimension;""").collect()
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.date_dimension (
            Date_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Date DATE,
            Month_Number INTEGER,
            Year_Number INTEGER,
            UNIQUE(Month_Number, Year_Number)
        )
    """).collect()
    session.sql(f"""
        INSERT INTO {schema_name}.date_dimension (Month_Number, Year_Number)
        SELECT DISTINCT cdd.MONTH, cdd.YEAR
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES AS pvp
        FULL OUTER JOIN
        CALENDAR_DATA_WITH_DATE_DIMENSIONS__FREE_READY_TO_USE.PUBLIC.CALENDAR_DATA AS cdd
        ON pvp.YEAR = cdd.YEAR
        WHERE cdd.MONTH IS NOT NULL AND cdd.YEAR IS NOT NULL
    """).collect()

# ----------------------------------------------------------------------------------------------------------------
    # Create and populate the Product dimension table
    session.sql(f"""DROP TABLE IF EXISTS {schema_name}.product_dimension;""").collect()
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.product_dimension (
            Product_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Product_Name VARCHAR(16777215),
            Main_Category VARCHAR(16777215),
            Sub_Category VARCHAR(16777215),
            Title VARCHAR(16777215),
            UNIQUE(Product_Name, Main_Category, Sub_Category, Title)
        )
    """).collect()
    session.sql(f"""
        INSERT INTO {schema_name}.product_dimension (Product_Name, Main_Category, Sub_Category, Title)
        SELECT
        LEFT(PRODUCT, 16777215),             -- Truncate Product_Name to 255 characters
        LEFT(MAIN_CATEGORY, 16777215),       -- Truncate Main_Category to 255 characters
        LEFT(SUB_CATEGORY, 16777215),        -- Truncate Sub_Category to 255 characters
        LEFT(TITLE, 16777215)                -- Truncate Title to 255 characters
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES
        WHERE PRODUCT IS NOT NULL
    """).collect()

# ----------------------------------------------------------------------------------------------------------------


    # Create and populate the Site dimension table
    session.sql(f"""DROP TABLE IF EXISTS {schema_name}.site_dimension;""").collect()
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.site_dimension (
            Site_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Site_Name VARCHAR(16777215) UNIQUE
        )
    """).collect()
    session.sql(f"""
        INSERT INTO {schema_name}.site_dimension (Site_Name)
        SELECT DISTINCT LEFT (SITE, 16777215)
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES
        WHERE SITE IS NOT NULL
    """).collect()

# ----------------------------------------------------------------------------------------------------------------


    return "Dimension tables have been created and populated."


# Function to create the fact table
def create_fact_table(session, schema_name):
    #Fact table
    # session.sql(f"""DROP TABLE IF EXISTS {schema_name}.fact_sales;""").collect()
    # session.sql(f"""
    #     CREATE TABLE IF NOT EXISTS {schema_name}.fact_sales (
    #     Fact_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    #     Brand_Name VARCHAR (16777215),
    #     MONTH INTEGER,
    #     YEAR INTEGER,
    #     PRODUCT_NAME VARCHAR (16777215),
    #     Estimated_Purchases FLOAT,
    #     Estimated_Views FLOAT
    #     );
    # """).collect()

    # # Populate the Fact table with data from the original table and keys from dimension tables
    # session.sql(f"""
    #     INSERT INTO {schema_name}.fact_sales (
    #     Brand_Name,MONTH,
    #     YEAR,
    #     PRODUCT_NAME, Estimated_Purchases, Estimated_Views)
    #     SELECT DISTINCT
    #     Brand,
    #     MONTH,
    #     YEAR,
    #     PRODUCT,
    #     ESTIMATED_PURCHASES,
    #     ESTIMATED_VIEWS
    #     FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES o


    # """).collect()


    session.sql(f"""DROP TABLE IF EXISTS {schema_name}.weekly_sales;""").collect()
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.weekly_sales (
        Sales_ID INTEGER AUTOINCREMENT PRIMARY KEY,
        IS_AVAILABLE FLOAT,
        RATINGS FLOAT,
        PRODUCT_NAME VARCHAR(16777215),
        PRICE FLOAT,
        REVIEW_COUNT INTEGER,
        SALES FLOAT,
        SALES_1P FLOAT,
        SALES_3P FLOAT,
        REVENUE FLOAT,
        REVENUE_1P FLOAT,
        REVENUE_3P FLOAT,
        category_rank INTEGER,
        subcategory_rank INTEGER
        );

    """).collect()
    session.sql(f"""
        INSERT INTO {schema_name}.weekly_sales (
        IS_AVAILABLE,
        RATINGS,
        PRODUCT_NAME,
        PRICE,
        REVIEW_COUNT,
        SALES,
        SALES_1P,
        SALES_3P,
        REVENUE,
        REVENUE_1P,
        REVENUE_3P,
        category_rank ,
        subcategory_rank
        )
        SELECT DISTINCT
        IS_AVAILABLE,
        RATINGS,
        NAME,
        PRICE,
        REVIEW_COUNT,
        SALES,
        SALES_1P,
        SALES_3P,
        REVENUE,
        REVENUE_1P,
        REVENUE_3P,
        category_rank ,
        subcategory_rank
        FROM
            AMAZON_SALES_AND_MARKET_SHARE_DEMO.SALES_ESTIMATES_SCHEMA.SALES_ESTIMATES_WEEKLY
        LIMIT 1000000;

    """).collect()


# Main function to orchestrate table creation and data insertion
def main(session, *args):
    schema_name = "PRODUCT_VIEWS_AND_PURCHASES_DIM_MODEL"
    print(create_schema(session, schema_name))   

    session.sql(f"USE SCHEMA {schema_name}").collect()
    
    #create_and_populate_dimension_tables(session, schema_name)
    create_fact_table(session, schema_name)

    return "Dimension and Fact tables have been created and populated."


# Entry point for the script
if __name__ == '__main__':
    import os, sys

    # Gets the directory where the current file is located
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Navigate up two levels to the root of your project
    root_dir = os.path.dirname(current_dir)

    # Adds the root directory to the sys.path to find myproject_utils
    sys.path.append(root_dir)

    # Imports snowpark_utils from your project utilities
    from myproject_utils import snowpark_utils
    # Gets the Snowpark session
    session = snowpark_utils.get_snowpark_session()

    # Try block to ensure that the session is closed properly
    try:
        # Execute the main function with any additional command line arguments
        if len(sys.argv) > 1:
            print(main(session, *sys.argv[1:]))  # Pass additional arguments if provided
        else:
            print(main(session))  # Call main without additional arguments
    finally:
        # Close the session
        session.close()