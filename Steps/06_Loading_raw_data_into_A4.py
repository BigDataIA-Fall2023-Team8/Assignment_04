import os
from snowflake.snowpark import Session


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

def create_and_populate_raw_data(session, schema_name):
    
    # Create and populate amazon_and_ecommerce table
    session.sql(f"""DROP TABLE IF EXISTS {schema_name}.amazon_and_ecommerce;""").collect()
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.amazon_and_ecommerce (
            ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Brand VARCHAR(16777215),
            Title VARCHAR(16777215),
            Main_Category VARCHAR(16777215),
            Sub_Category VARCHAR(16777215),
            Estimated_Views FLOAT,
            Estimated_Purchases FLOAT,
            Year INTEGER, 
            Month INTEGER,
            Site VARCHAR(16777215)
        )
    """).collect()
    session.sql(f"""
        INSERT INTO {schema_name}.amazon_and_ecommerce (Site, Brand, Title, Main_Category, Sub_Category, Estimated_Views, Estimated_Purchases, Year, Month)
        SELECT Site, Brand, Title, Main_Category, Sub_Category, Estimated_Views, Estimated_Purchases, Year, Month
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES 
        LIMIT 10000000;
    """).collect()
    session.sql(f"""
    ALTER TABLE {schema_name}.amazon_and_ecommerce RENAME COLUMN Title TO Product;
""").collect()

# ----------------------------------------------------------------------------------------------------------------
    # Create and populate the amazon sales table
    session.sql(f"""DROP TABLE IF EXISTS {schema_name}.amazon_sales_weekly;""").collect()
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.amazon_sales_weekly (
            ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Marketplace VARCHAR(16777215),
            Brand VARCHAR(16777215),
            IS_AVAILABLE FLOAT,
            Ratings FLOAT,
            Seller_IDs ARRAY,
            Seller_Types ARRAY,
            Name VARCHAR(16777215),
            Price FLOAT,
            review_count INTEGER,
            sales FLOAT,
            sales_1P FLOAT,
            sales_3P FLOAT,
            revenue FLOAT,
            revenue_1P FLOAT,
            revenue_3P FLOAT
            
        )
    """).collect()
    session.sql(f"""
        INSERT INTO {schema_name}.amazon_sales_weekly (Marketplace, Brand, IS_AVAILABLE, Ratings,  Seller_IDs, Seller_Types, Name, Price, review_count, sales, sales_1P, sales_3P, revenue, revenue_1P, revenue_3P)
        SELECT Marketplace, Brand, IS_AVAILABLE, Ratings AS Reviews, Seller_IDs, Seller_Types, Name AS Product, Price, review_count, sales, sales_1P, sales_3P, revenue, revenue_1P, revenue_3P
        FROM AMAZON_SALES_AND_MARKET_SHARE_DEMO.SALES_ESTIMATES_SCHEMA.SALES_ESTIMATES_WEEKLY
        LIMIT 10000000;
    """).collect()
    session.sql(f"""
    ALTER TABLE {schema_name}.amazon_sales_weekly RENAME COLUMN Name TO Product;
""").collect()
    session.sql(f"""
    ALTER TABLE {schema_name}.amazon_sales_weekly RENAME COLUMN Ratings TO Reviews;
""").collect()
    session.sql(f"""
    ALTER TABLE {schema_name}.amazon_sales_weekly RENAME COLUMN sales_1P TO sales_1stParty;
""").collect()
    session.sql(f"""
    ALTER TABLE {schema_name}.amazon_sales_weekly RENAME COLUMN sales_3P TO sales_3rdParty;
""").collect()
    session.sql(f"""
    ALTER TABLE {schema_name}.amazon_sales_weekly RENAME COLUMN revenue_1P TO revenue_1stParty;
""").collect()
    session.sql(f"""
    ALTER TABLE {schema_name}.amazon_sales_weekly RENAME COLUMN revenue_3P TO revenue_3rdParty;
""").collect()
# -------------------------------------------------------------------------------------
    # Create and populate the date table
    session.sql(f"""DROP TABLE IF EXISTS {schema_name}.date_data;""").collect()
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.date_data (
            Date DATE,
            Year INTEGER,
            Month INTEGER,
            MonthName VARCHAR(16777215),
            Day INTEGER,
            DayName VARCHAR(16777215),
            Quarter INTEGER,
            Week INTEGER,
            DayOfMonth INTEGER,
            DayOfWeek INTEGER,
            DayOfYear INTEGER,
            Last_Day_Of_Quarter Date,
            WeekOfYear INTEGER,
            Day_Of_Quarter INTEGER
            
        )
    """).collect()
    session.sql(f"""
        INSERT INTO {schema_name}.date_data (Date,Year,Month,MonthName,Day,DayName,Quarter,Week,DayOfMonth,DayOfWeek,DayOfYear,Last_Day_Of_Quarter,WeekOfYear,Day_Of_Quarter)
        SELECT Date,Year,Month,MonthName,Day,DayName,Quarter,Week,DayOfMonth,DayOfWeek,DayOfYear,Last_Day_Of_Quarter,WeekOfYear,Day_Of_Quarter
        FROM CALENDAR_DATA_WITH_DATE_DIMENSIONS__FREE_READY_TO_USE.PUBLIC.CALENDAR_DATA
        LIMIT 10000000;
    """).collect()

    return "Dimension tables have been created and populated."




# Main function to orchestrate table creation and data insertion
def main(session, *args):
    schema_name = "Staging"
    # print(create_schema(session, schema_name))   

    session.sql(f"USE SCHEMA {schema_name}").collect()
    
    create_and_populate_raw_data(session, schema_name)
    # create_fact_table(session, schema_name)

    return "Raw Data Loaded."

# Entry point for the script
if __name__ == '__main__':
    import os, sys

    # Gets the directory where the current file is located
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Navigate up two levels to the root of your project
    root_dir = os.path.dirname(current_dir)

    # Adds the root directory to the sys.path to find myproject_utils
    sys.path.append(root_dir)


    # snowpark_utils
    # # Imports snowpark_utils from your project utilities
    # from myproject_utils import snowpark_utils
    # # Gets the Snowpark session
    # session = snowpark_utils.get_snowpark_session()
    connection_parameters = {
        "user": "sohamd148",
        "password": "Mahos@14899",
        "account": "lab93413.us-east-1",
        "warehouse": "A4_WH",
        "database": "A4_DB",
        "schema": "Public"
    }
    session = Session.builder.configs(connection_parameters).create()

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