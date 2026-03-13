from db_connect import db_connect

def create_dim_dates():
    # open the SQL connection, it requires host, port, database_name, username, and password
    conn = db_connect()

    # create a cursor object to interact with the database
    cursor = conn.cursor()

    # create the dim_dates table if not exists, otherwise do nothing
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date DATE PRIMARY KEY,
            day INT,
            month INT,
            year INT,
            quarter INT,
            day_of_week INT,
            week_of_year INT
        );
        """
    )

    conn.commit()  # commit the transaction to save changes
    cursor.close()  # close the cursor
    conn.close()  # close the connection

def create_dim_metadata():
    conn = db_connect()

    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_metadata (
            symbol TEXT PRIMARY KEY,
            company_name TEXT,
            sector TEXT
            );
        """       
    )

    conn.commit()   # 
    cursor.close()
    conn.close()

# run the functions to create the dimension tables when this script is executed directly, not from the orchestration script (python entry point guard)
if __name__ == "__main__":
    create_dim_dates()
    create_dim_metadata()