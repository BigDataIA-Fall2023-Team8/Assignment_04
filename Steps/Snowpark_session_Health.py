from snowflake.snowpark import Session

def main():
    connection_parameters = {
        "user": "sohamd148",
        "password": "Mahos@14899",
        "account": "lab93413.us-east-1",
        "warehouse": "A4_WH",
        "database": "A4_DB",
        "schema": "Public"
    }

    try:
        session = Session.builder.configs(connection_parameters).create()
        # Test query to check session
        result = session.sql("SELECT CURRENT_VERSION()").collect()
        print("Snowpark session is active. Snowflake version:", result[0][0])
    except Exception as e:
        print("Error in Snowpark session:", e)
    finally:
        session.close()

if __name__ == "__main__":
    main()
