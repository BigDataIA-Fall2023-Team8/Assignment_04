import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import ArrayType, StringType
from snowflake.snowpark.types import FloatType


# Load environment variables
# snowflake_user = os.getenv('SNOWFLAKE_USER')
# snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
# snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
# snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
# snowflake_database = os.getenv('SNOWFLAKE_DATABASE')
# snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')
# openai_api_key = os.getenv('OPENAI_API_KEY')

from snowflake.snowpark.functions import col, when, lit, call_udf
 
# Function to create schema
# def create_schema(session, schema_name):
#     session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}").collect()
#     return f"Schema '{schema_name}' has been created."
 
# Function to clean and transform the data
def transform_data_amazon_and_ecommerce(session, source_table, schema_name, target_table):
    # Read data from the source table
    df = session.table(f"{schema_name}.{source_table}")
 
     # Replace null BRAND with 'Brand not available'
    df = df.withColumn('BRAND', when(col('BRAND').isNull(), lit('Brand not available')).otherwise(col('BRAND')))

    # df = df.withColumn('Year', 
    #                when(col('Year') == '21', '2021')
    #                .when(col('Year') == '22', '2022')
    #                .otherwise(col('Year')))
 
    # Drop rows where NAME is null
    df = df.filter(col('Product').isNotNull())
    df = df.withColumn("conversion_rate", call_udf("calculate_conversion_rate", col("estimated_views"), col("estimated_purchases")))
    df = df.withColumn("season", call_udf("month_to_season", col("month")))
    # Write the transformed data into the target table
    df.write.saveAsTable(f"{schema_name}.{target_table}", mode='overwrite')
 
    return "Data has been transformed and loaded into the amazon_and_ecommerce table."

def transform_data_amazon_sales_weekly(session, source_table, schema_name, target_table):
    
    def transform_seller_types(seller_types):
    # Define the mapping
        replacements = {
            "[AMZ]": 'Amazon',
            "[FBA]": 'Fulfilled by Amazon',
            "[FBM]": 'Fulfilled by Merchants'
        }

    # Transform the array
        return [replacements.get(st, st) for st in seller_types]
    
    transform_seller_types_udf = udf(transform_seller_types, 
                                    return_type=ArrayType(StringType()),
                                    input_types=[ArrayType(StringType())])
    
        
    # Read data from the source table
    df = session.table(f"{schema_name}.{source_table}")
    
     # Replace null BRAND with 'Brand not available'
    df = df.withColumn('BRAND', when(col('BRAND').isNull(), lit('Brand not available')).otherwise(col('BRAND')))
    df = df.withColumn('SELLER_TYPES', transform_seller_types_udf(col('SELLER_TYPES')))
    df = df.filter(col('Product').isNotNull())
    
    # Write the transformed data into the target table
    df.write.saveAsTable(f"{schema_name}.{target_table}", mode='overwrite')
 
    return "Data has been transformed and loaded into the amazon_sales_weekly table."
 


# Main function to orchestrate the schema creation and data transformation
def main(session, *args):
    schema_name = 'Staging'
    source_table1 = 'amazon_and_ecommerce'
    target_table1 = 'amazon_and_ecommerce'
    source_table2 = 'amazon_sales_weekly'
    target_table2 = 'amazon_sales_weekly'
    # Register the UDF in Snowflake
    
    # print(create_schema(session, schema_name))
    print(transform_data_amazon_and_ecommerce(session, source_table1, schema_name, target_table1))
    print(transform_data_amazon_sales_weekly(session, source_table2, schema_name, target_table2))
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

    connection_parameters = {
        "user": "sohamd148",
        "password": "Mahos@14899",
        "account": "lab93413.us-east-1",
        "warehouse": "A4_WH",
        "database": "A4_DB",
        "schema": "Staging"
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
    
