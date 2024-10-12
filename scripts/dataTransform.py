import pandas as pd
import logging

# Set up logging
logging.basicConfig(filename='./log/data_transformation.log', level=logging.INFO)

# Step 1: Load the cleaned data
def load_cleaned_data(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()  # Ensure no leading/trailing spaces in column names
        logging.info(f'Cleaned data loaded successfully from {file_path}.')
        return df
    except Exception as e:
        logging.error(f'Error loading cleaned data: {e}')
        raise

# Step 2: Transform the data
def transform_data(df):
    # Example transformation: Add new features
    if 'date' in df.columns:
        df['year'] = pd.DatetimeIndex(df['date']).year
        df['month'] = pd.DatetimeIndex(df['date']).month
        df['day'] = pd.DatetimeIndex(df['date']).day
        logging.info('Extracted year, month, and day from the date column.')
    else:
        logging.warning("Column 'date' not found for transformation.")

    if 'text' in df.columns:
        df['text_length'] = df['text'].apply(lambda x: len(str(x)))
        logging.info('Calculated text length for each message.')
    else:
        logging.warning("Column 'text' not found for transformation.")

    if 'channel' in df.columns:
        df['channel'] = df['channel'].str.lower()
        logging.info('Standardized channel column to lowercase.')
    else:
        logging.warning("Column 'channel' not found for transformation.")

    return df

# Step 3: Save transformed data to CSV
def save_transformed_data_to_csv(df, output_path):
    try:
        df.to_csv(output_path, index=False)  # Save without the index
        logging.info(f'Transformed data saved successfully to {output_path}.')
    except Exception as e:
        logging.error(f'Error saving transformed data: {e}')
        raise

# Step 4: Save transformed data to SQL format
def save_transformed_data_to_sql(df, output_sql_path, table_name):
    try:
        # Prepare SQL statements
        sql_statements = []
        for _, row in df.iterrows():
            values = "', '".join(str(x).replace("'", "''") for x in row)  # Handle single quotes in data
            sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ('{values}');"
            sql_statements.append(sql)

        # Write the SQL statements to a file
        with open(output_sql_path, 'w') as f:
            f.write('\n'.join(sql_statements))
        logging.info(f'Transformed data saved successfully to {output_sql_path}.')
    except Exception as e:
        logging.error(f'Error saving transformed data to SQL: {e}')
        raise

# Main function to orchestrate the transformation process
def main_transformation():
    file_path = './data/cleaned_telegram_data.csv'  # Input cleaned CSV file
    output_csv_path = './data/transformed_telegram_data.csv'  # Output transformed CSV file
    output_sql_path = './data/transformed_telegram_data.sql'  # Output SQL file
    table_name = 'telegram_messages'  # Replace with your table name

    # Load, transform, and save data
    df = load_cleaned_data(file_path)
    df_transformed = transform_data(df)
    
    save_transformed_data_to_csv(df_transformed, output_csv_path)
    save_transformed_data_to_sql(df_transformed, output_sql_path, table_name)
    
    logging.info('Data transformation completed successfully.')
    return df_transformed

if __name__ == '__main__':
    df_transformed = main_transformation()
