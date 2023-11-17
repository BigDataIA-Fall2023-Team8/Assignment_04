import sys
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import FloatType

import os

# Load environment variables
snowflake_user = os.getenv('SNOWFLAKE_USER')
snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
snowflake_database = os.getenv('SNOWFLAKE_DATABASE')
snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')
openai_api_key = os.getenv('OPENAI_API_KEY')


# Define the Python function to calculate conversion rate
def calculate_conversion_rate(estimated_views: float, estimated_purchases: float) -> float:
    if estimated_views > 0:  # Prevent division by zero
        return (estimated_purchases / estimated_views) * 100
    else:
        return 0.0  # If no views, return 0

# Register the function as a UDF in Snowflake
# conversion_rate_udf = udf(calculate_conversion_rate, return_type=FloatType(), input_types=[FloatType(), FloatType()])

if __name__ == '__main__':
    # For local debugging
    if len(sys.argv) > 2:
        views_arg = float(sys.argv[1])  # Convert the first argument to a float
        purchases_arg = float(sys.argv[2])  # Convert the second argument to a float
        print(calculate_conversion_rate(views_arg, purchases_arg))
    else:
        print("Insufficient arguments provided. Please provide estimated views and purchases.")