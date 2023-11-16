# Function to create dimension tables
def create_and_populate_dimension_tables(session):
    # Create and populate the Brand dimension table
    session.sql("""
        CREATE TABLE IF NOT EXISTS brand_dimension (
            Brand_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Brand_Name VARCHAR(255) UNIQUE
        )
    """).collect()
    session.sql("""
        INSERT INTO brand_dimension (Brand_Name)
        SELECT DISTINCT BRAND
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES
        WHERE BRAND IS NOT NULL
    """).collect()

    # Create and populate the Country dimension table
    session.sql("""
        CREATE TABLE IF NOT EXISTS country_dimension (
            Country_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Country_Number INTEGER UNIQUE
        )
    """).collect()
    session.sql("""
        INSERT INTO country_dimension (Country_Number)
        SELECT DISTINCT COUNTRY
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES
        WHERE COUNTRY IS NOT NULL
    """).collect()

    # Create and populate the Date dimension table
    session.sql("""
        CREATE TABLE IF NOT EXISTS date_dimension (
            Date_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Month_Number INTEGER,
            Year_Number INTEGER,
            UNIQUE(Month_Number, Year_Number)
        )
    """).collect()
    session.sql("""
        INSERT INTO date_dimension (Month_Number, Year_Number)
        SELECT DISTINCT MONTH, YEAR
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES
        WHERE MONTH IS NOT NULL AND YEAR IS NOT NULL
    """).collect()

    # Create and populate the Product dimension table
    session.sql("""
        CREATE TABLE IF NOT EXISTS product_dimension (
            Product_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Product_Name VARCHAR(255),
            Main_Category VARCHAR(255),
            Sub_Category VARCHAR(255),
            Title VARCHAR(255),
            UNIQUE(Product_Name, Main_Category, Sub_Category, Title)
        )
    """).collect()
    session.sql("""
        INSERT INTO product_dimension (Product_Name, Main_Category, Sub_Category, Title)
        SELECT DISTINCT PRODUCT, MAIN_CATEGORY, SUB_CATEGORY, TITLE
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES
        WHERE PRODUCT IS NOT NULL
    """).collect()

    # Create and populate the Site dimension table
    session.sql("""
        CREATE TABLE IF NOT EXISTS site_dimension (
            Site_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Site_Name VARCHAR(255) UNIQUE
        )
    """).collect()
    session.sql("""
        INSERT INTO site_dimension (Site_Name)
        SELECT DISTINCT SITE
        FROM AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES
        WHERE SITE IS NOT NULL
    """).collect()

    return "Dimension tables have been created and populated."


# Function to create the fact table
def create_fact_table(session):
    # Fact table
    session.sql("""
        CREATE TABLE IF NOT EXISTS fact_sales (
            Fact_ID INTEGER AUTOINCREMENT PRIMARY KEY,
            Date_ID INTEGER,
            Brand_ID INTEGER,
            Country_ID INTEGER,
            Product_ID INTEGER,
            Site_ID INTEGER,
            Estimated_Purchases FLOAT,
            Estimated_Views FLOAT,
            FOREIGN KEY (Date_ID) REFERENCES date_dimension(Date_ID),
            FOREIGN KEY (Brand_ID) REFERENCES brand_dimension(Brand_ID),
            FOREIGN KEY (Country_ID) REFERENCES country_dimension(Country_ID),
            FOREIGN KEY (Product_ID) REFERENCES product_dimension(Product_ID),
            FOREIGN KEY (Site_ID) REFERENCES site_dimension(Site_ID)
        )
    """).collect()

    # Populate the Fact table with data from the original table and keys from dimension tables
    session.sql("""
        INSERT INTO fact_sales (Date_ID, Brand_ID, Country_ID, Product_ID, Site_ID, Estimated_Purchases, Estimated_Views)
        SELECT
            dd.Date_ID,
            bd.Brand_ID,
            cd.Country_ID,
            pd.Product_ID,
            sd.Site_ID,
            o.ESTIMATED_PURCHASES,
            o.ESTIMATED_VIEWS
        FROM
            AMAZON_AND_ECOMMERCE_WEBSITES_PRODUCT_VIEWS_AND_PURCHASES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES o
        JOIN date_dimension dd ON o.MONTH = dd.Month_Number AND o.YEAR = dd.Year_Number
        JOIN brand_dimension bd ON o.BRAND = bd.Brand_Name
        JOIN country_dimension cd ON o.COUNTRY = cd.Country_Number
        JOIN product_dimension pd ON o.PRODUCT = pd.Product_Name
        JOIN site_dimension sd ON o.SITE = sd.Site_Name
    """).collect()


# Main function to orchestrate table creation and data insertion
def main(session, *args):
    create_and_populate_dimension_tables(session)
    create_fact_table(session)
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
