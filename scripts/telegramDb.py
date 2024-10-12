import psycopg2
import logging

# Set up logging
logging.basicConfig(filename='./log/data_insertion.log', level=logging.INFO)

# Step 1: Create PostgreSQL connection
def create_db_connection():
    try:
        # Replace with your actual PostgreSQL credentials
        conn = psycopg2.connect(
            dbname="tlgdata",   # Replace with your database name
            user="postgres",          # Replace with your PostgreSQL username
            password="pass@1q2w",      # Replace with your PostgreSQL password
            host="localhost",              # Replace with your database host (e.g., 'localhost')
            port="5432"                    # Replace with your database port (default is 5432)
        )
        logging.info("Database connection established successfully.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise

# Step 2: Create the table if it doesn't exist
def create_table(conn):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS telegram_messages (
        channel VARCHAR(255),
        message_id VARCHAR(255) PRIMARY KEY,
        date TIMESTAMP,
        text TEXT,
        media VARCHAR(255),
        year INTEGER,
        month INTEGER,
        day INTEGER,
        text_length INTEGER
    );
    '''
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
            logging.info("Table 'telegram_messages' created successfully (if not exists).")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        conn.rollback()
        raise

# Step 3: Read and execute SQL file with cleaning
def execute_sql_file(sql_file_path, conn):
    try:
        with open(sql_file_path, 'r') as f:
            sql_commands = f.read().splitlines()

        with conn.cursor() as cursor:
            for command in sql_commands:
                command = command.strip()  # Remove any surrounding whitespace

                # Check if command is not empty and starts with 'INSERT'
                if command and command.startswith("INSERT"):
                    try:
                        # Split command into parts
                        command_parts = command.split("VALUES (", 1)
                        if len(command_parts) != 2:
                            logging.warning(f"Malformed command, skipping: {command}")
                            continue

                        values_part = command_parts[1].strip().strip(");")
                        # Prepare the values for insertion
                        values = values_part.split(", ")

                        # Escape single quotes in each value
                        values = [value.strip().strip("'").replace("'", "''") for value in values]  # Escape single quotes

                        # Log the command to be executed
                        logging.info(f"Executing SQL command: {command} with values: {values}")

                        # Create a parameterized insert command
                        insert_command = '''
                        INSERT INTO telegram_messages (channel, message_id, date, text, media, year, month, day, text_length)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        '''

                        # Execute the parameterized command
                        cursor.execute(insert_command, values)
                    except IndexError as ie:
                        logging.error(f"Index error while processing command: {command}. Error: {ie}")
                    except Exception as e:
                        logging.error(f"Error executing SQL command: {command}. Error: {e}")
                else:
                    logging.warning(f"Skipped malformed or empty command: {command}")

            conn.commit()  # Commit all changes after executing the commands
            logging.info(f"SQL file {sql_file_path} executed successfully.")
    except Exception as e:
        logging.error(f"Error executing SQL file: {e}")
        conn.rollback()  # Roll back in case of error
        raise

# Step 4: Main function to orchestrate the process
def main_insertion():
    sql_file_path = './data/transformed_telegram_data.sql'  # Path to the SQL file

    # Step 1: Connect to the database
    conn = create_db_connection()

    try:
        # Step 2: Create the table
        create_table(conn)

        # Step 3: Execute the SQL commands in the file
        execute_sql_file(sql_file_path, conn)
    finally:
        # Step 4: Close the database connection
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == '__main__':
    main_insertion()
