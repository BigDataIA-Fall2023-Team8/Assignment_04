import os
import streamlit as st
import snowflake.connector
from snowflake.connector import ProgrammingError
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from langchain.chat_models import ChatOpenAI
from dotenv import load_dotenv
from langchain.utilities import SQLDatabase
from langchain.llms import OpenAI  # Correct import for OpenAI
# from langchain.chains import SQLDatabaseChain
from dotenv import load_dotenv
from langchain_experimental.sql import SQLDatabaseChain
from langchain.chains import create_sql_query_chain
from langchain.prompts import PromptTemplate


# Load environment variables from .env file
load_dotenv()

# Function to establish connection to Snowflake using Snowflake Connector
def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        return conn
    except ProgrammingError as e:
        st.error(f'Error connecting to Snowflake: {e}')
        return None

# Establishing connection using SQLAlchemy
engine = create_engine(URL(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
))

db = SQLDatabase.from_uri(URL(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
))

# Streamlit user interface
def main():
    st.title('Data-Driven Decision Making with Snowflake')

    # Connect to Snowflake
    conn = get_snowflake_connection()
    if conn is not None:
        st.success('Connected to Snowflake!')

        # LangChain setup
        openai_api_key = os.getenv('OPENAI_API_KEY')
        if openai_api_key is None:
            st.error("OPENAI_API_KEY environment variable not set")
            return
        
        # # LangChain Query Generation
        # lc_query = st.text_input('Enter your LangChain question here:')
        # generated_sql_query = ''  # Variable to hold the generated SQL query
        # chain = create_sql_query_chain(ChatOpenAI(temperature=0), db)
        
        # if st.button('Generate SQL Query'):
        #     # Generate the SQL query using LangChain
        #     # chain = create_sql_query_chain(ChatOpenAI(temperature=0), db)
        #     response = chain.invoke({"question": lc_query})
        #     st.write(response)

        #     # Update the variable with the generated SQL query
        #     generated_sql_query = response

        # SQL Query Execution
        # The text area now uses the variable 'generated_sql_query' for its value
        # query = st.text_area('Execute your SQL query:', value=generated_sql_query)
        # if generated_sql_query:
        #     query = st.text_area('Execute your SQL query:', value=generated_sql_query)
        #     if st.button('Execute Query'):
        #         try:
        #             # Execute the query directly using your database connection
        #             with db.connection() as conn:  # Assuming 'db' is your database connection object
        #                 with conn.cursor() as cur:
        #                     cur.execute(query)
        #                     result = cur.fetchall()
        #                     st.write(result)
        #         except Exception as e:  # Catch a general exception
        #             st.error(f'Error executing query: {e}')
       
        #SQL_QUERY_CHAIN - Session_State
        # TEMPLATE = """Given an input question, first create a syntactically correct {dialect} query to run, then look at the results of the query and return the answer.
        # Use the following format:

        # Question: "Question here"
        # SQLQuery: "SQL Query to run"
        # SQLResult: "Result of the SQLQuery"
        # Answer: "Final answer here"

        # Only use the following tables:

        # {table_info}.

        # Some examples of SQL queries that correspond to questions are:

        # {few_shot_examples}

        # Question: {input}"""

        # # Define the table info string
        # table_info = """
        # BRAND_DIMENSION:
        # - BRAND_ID: Number
        # - BRAND_NAME: Varchar

        # COUNTRY_DIMENSION:
        # - COUNTRY_ID: Number
        # - COUNTRY_NUMBER: Number

        # DATE_DIMENSION:
        # - DATE: Date
        # - DATE_ID: Number
        # - MONTH_NUMBER: Number
        # - YEAR_NUMBER: Number

        # FACT_SALES:
        # - BRAND_NAME: Varchar
        # - ESTIMATED_PURCHASES: Float
        # - ESTIMATED_VIEWS: Float
        # - FACT_ID: Number
        # - MONTH: Number
        # - PRODUCT_NAME: Varchar
        # - YEAR: Number

        # PRODUCT_DIMENSION:
        # - MAIN_CATEGORY: Varchar
        # - PRODUCT_ID: Number
        # - PRODUCT_NAME: Varchar
        # - SUB_CATEGORY: Varchar
        # - TITLE: Varchar

        # SITE_DIMENSION:
        # - SITE_ID: Number
        # - SITE_NAME: Varchar

        # WEEKLY_SALES:
        # - IS_AVAILABLE: Float
        # - PRICE: Float
        # - PRODUCT_NAME: Varchar
        # - RATINGS: Float
        # - REVENUE: Float
        # - REVENUE_1P: Float
        # - REVENUE_3P: Float
        # - REVIEW_COUNT: Number
        # - SALES: Float
        # - SALES_1P: Float
        # - SALES_3P: Float
        # - SALES_ID: Number
        # """.strip()

        #1st approach
        chain = create_sql_query_chain(ChatOpenAI(temperature=0), db)

        # Initialize session state variable
        if 'generated_sql_query' not in st.session_state:
            st.session_state['generated_sql_query'] = ''

        # Text input for LangChain query
        lc_query = st.text_input('Enter your LangChain question here:')

        if st.button('Generate SQL Query'):
            # Generate the SQL query using LangChain
            response = chain.invoke({"question": lc_query})
            st.write(response)

            # Update the session state variable with the generated SQL query
            st.session_state['generated_sql_query'] = response

            #For template 
            # filled_prompt = custom_prompt.fill({
            #     "input": lc_query,
            #     "few_shot_examples": "Your few-shot examples here",  # Replace with actual examples
            #     "table_info": table_info,
            #     "dialect": "Standard SQL"  # Or any other dialect if needed
            # })
            # response = chain.invoke({"question": filled_prompt})
            # st.write(response)


        # SQL Query Execution - Only show if a query has been generated
        if st.session_state['generated_sql_query']:
            query = st.text_area('Execute your SQL query:', value=st.session_state['generated_sql_query'])
            if st.button('Execute Query'):
                try:
                    # Execute the query using the SQLAlchemy engine
                    with engine.connect() as conn:
                        result = conn.execute(query).fetchall()
                        st.write("Query Results:")
                        st.write(result)
                except Exception as e:
                    st.error(f'Error executing query: {e}')
        
        #---------------------------------------------------------------------------------------------------------------

        # # 2nd approach
        # # SQL SQLDATABASE CHAIN -SESSION STATE
        # # Import additional libraries for database connection

        # # Initialize the LLM and SQLDatabaseChain
        # llm = OpenAI(temperature=0, verbose=True)
        # db_chain = SQLDatabaseChain.from_llm(llm, db, verbose=True)  # Assuming 'db' is your database object

        # # Initialize session state for the generated SQL query
        # if 'generated_sql_query' not in st.session_state:
        #     st.session_state['generated_sql_query'] = ''

        # # Text input for LangChain query
        # lc_query = st.text_input('Enter your prompt here:')

        # if st.button('Generate SQL Query'):
        #     # Generate the SQL query using SQLDatabaseChain
        #     response = db_chain.run(lc_query)
        #     st.write("Generated Query:")
        #     st.write(response)

        #     # Update session state with the generated SQL query
        #     st.session_state['generated_sql_query'] = response

        # # Execute SQL Query - Only shown if a query has been generated
        # if st.session_state['generated_sql_query']:
        #     query = st.text_area('Execute your SQL query:', value=st.session_state['generated_sql_query'])
        #     if st.button('Execute Query'):
        #         try:
        #             # Execute the query using your database connection
        #             # Replace this with your database execution code
        #             with db.connection() as conn:  # Replace 'db.connection()' with your actual connection method
        #                 with conn.cursor() as cur:
        #                     cur.execute(query)
        #                     result = cur.fetchall()
        #                     st.write("Query Results:")
        #                     st.write(result)
        #         except Exception as e:
        #             st.error(f'Error executing query: {e}')

if __name__ == '__main__':
    main()
