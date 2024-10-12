import pandas as pd
import logging

# Set up logging
logging.basicConfig(filename='./log/data_cleaning.log', level=logging.INFO)

# Step 1: Load the data
def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()  # Remove extra spaces from column names
        logging.info(f'Data loaded successfully from {file_path}.')
        print("Column names in the CSV file:", df.columns)  # Print the column names
        return df
    except Exception as e:
        logging.error(f'Error loading data: {e}')
        raise

# Step 2: Data Cleaning
def clean_data(df):
    # Remove duplicates
    initial_row_count = df.shape[0]
    df.drop_duplicates(inplace=True)
    logging.info(f'Removed {initial_row_count - df.shape[0]} duplicate rows.')

    # Handle missing values
    missing_count = df.isnull().sum().sum()
    df.ffill(inplace=True)  # Forward fill as an example
    logging.info(f'Filled missing values. Total missing values before: {missing_count}, after: {df.isnull().sum().sum()}.')

    # Standardize date format
    if 'date' in df.columns:  # Assuming the column is 'date'
        df['date'] = pd.to_datetime(df['date'], errors='coerce')  # Convert to datetime and coerce errors
        logging.info('Date column standardized.')
    else:
        logging.warning("Column 'date' not found in the data.")

    # Validate numeric ranges (if there's a numeric column)
    if 'message_id' in df.columns:  # Assume 'message_id' should be a valid numeric range
        if df['message_id'].between(1, 1e6).all():  # Example range for 'message_id'
            logging.info("'message_id' column values are within the valid range.")
        else:
            logging.error("Some 'message_id' values are out of bounds.")
    else:
        logging.warning("Column 'message_id' not found in the data.")

    return df

# Step 3: Save cleaned data
def save_cleaned_data(df, output_path):
    try:
        df.to_csv(output_path, index=False)  # Save without the index
        logging.info(f'Cleaned data saved successfully to {output_path}.')
    except Exception as e:
        logging.error(f'Error saving cleaned data: {e}')
        raise

# Main function to orchestrate the cleaning process
def main_cleaning():
    file_path = './data/telegram_scraped_data.csv'  
    output_path = './data/cleaned_telegram_data.csv'  

    df = load_data(file_path)
    df_cleaned = clean_data(df)
    
    save_cleaned_data(df_cleaned, output_path)
    logging.info('Data cleaning completed successfully.')
    return df_cleaned

if __name__ == '__main__':
    df_cleaned = main_cleaning()
