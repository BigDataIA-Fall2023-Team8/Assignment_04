import os

# Load environment variables
snowflake_user = os.getenv('SNOWFLAKE_USER')
snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
snowflake_database = os.getenv('SNOWFLAKE_DATABASE')
snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')
openai_api_key = os.getenv('OPENAI_API_KEY')

from snowflake.snowpark.functions import col, when, lit
 
# Function to create schema
def create_schema(session, schema_name):
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}").collect()
    return f"Schema '{schema_name}' has been created."
 
# Function to clean and transform the data
def transform_data(session, source_table, schema_name, target_table):
    # Read data from the source table
    df = session.table(f"{schema_name}.{source_table}")
 
    # Replace null BRAND with 'Brand not available'
    df = df.withColumn('BRAND', when(col('BRAND').isNull(), lit('Brand not available')).otherwise(col('BRAND')))
    # Replace null CATEGORY_RANK, SUBCATEGORY_RANK, and RATINGS with 0
    df = df.withColumn('CATEGORY_RANK', when(col('CATEGORY_RANK').isNull(), lit(0)).otherwise(col('CATEGORY_RANK')))
    df = df.withColumn('SUBCATEGORY_RANK', when(col('SUBCATEGORY_RANK').isNull(), lit(0)).otherwise(col('SUBCATEGORY_RANK')))
    df = df.withColumn('RATINGS', when(col('RATINGS').isNull(), lit(0)).otherwise(col('RATINGS')))
 
    # Replace seller types codes with full text
    #df = df.withColumn('SELLER_TYPES', col('SELLER_TYPES').replace('[AMZ]', 'Amazon').replace('[FBA]', 'Fulfilled by Amazon').replace('[FBM]', 'Fulfilled by Merchants'))
 
    # Drop rows where NAME is null
    df = df.filter(col('product_NAME').isNotNull())
 
    # Write the transformed data into the target table
    df.write.saveAsTable(f"{schema_name}.{target_table}", mode='overwrite')
 
    return "Data has been transformed and loaded into the target table."
 
# Main function to orchestrate the schema creation and data transformation
def main(session, *args):
    schema_name = 'product_views_and_purchases_dim_model'
    source_table = 'weekly_sales'
    target_table = 'weekly_sales'
    print(create_schema(session, schema_name))
    print(transform_data(session, source_table, schema_name, target_table))
    return "ETL process completed successfully."
 
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
    
