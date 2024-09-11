import pandas as pd
import os

# Create a location dictionary
location_dict = {
    'A': 'Atlantic mall (Chevron)',
    'B': '1004 (VI)',
    'C': 'Ayangbure (Main)',
    'D': 'Ayangbure Stand',
    'E': 'Dream Park (Main)',
    'F': 'Dream Park Stand',
    'G': 'Club (Ayangbure)',
    'H': 'Hotel (Ayangbure)',
    'L': 'Lounge (Ayangbure)'
}

# Ask user to input date
date_input = input("Enter the date (DD-MM-YYYY): ")

# Ask user to enter location code
location_code = input("Enter the location code (A-L): ")

# Validate the location code
if location_code.upper() not in location_dict:
    print("Invalid location code, please try again.")
else:
    # Set the folder where CSV files are located
    folder_path = '/Users/davidabejide/Documents/GitHub/code-scripts'

    # List to hold all processed DataFrames
    combined_df_list = []

    # Loop through all CSV files in the directory
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            # Read the CSV file
            df = pd.read_csv(os.path.join(folder_path, file_name))

            # Print columns to help identify any issues
            print(f"Processing {file_name} with columns: {df.columns}")

            # Update DataFrame as per the original logic, ensuring the correct column name is used
            if 'Name' in df.columns:  # Check if 'Name' exists
                df['Item(Product/Service)'] = df['Name']
            else:
                print(f"'Name' column not found in {file_name}")
                continue  # Skip this file if 'Name' column is missing

            df['*InvoiceNo'] = location_code.upper() + date_input
            df['*Customer'] = 'Walk In Customer'
            df['*InvoiceDate'] = date_input
            df['*DueDate'] = date_input
            df['Terms'] = 'Due on receipt'
            df['Location'] = location_dict[location_code]
            df['Memo'] = ''
            df['ItemDescription'] = df['Description']
            df['ItemQuantity'] = df['Qty']
            df['ItemRate'] = ''
            df['*ItemAmount'] = df['ValueIncVAT']
            df['*ItemTaxCode'] = df['Name'].apply(
                lambda x: 'No VAT' if 'delivery' in x.lower() or 'pack' in x.lower() else 'Sales Tax')
            df['*ItemTaxAmount'] = ''
            df['Service Date'] = date_input

            # Add the new column to the first position
            df.insert(0, 'Date', date_input)

            # Remove the last row
            df = df.drop(df.index[-1])

            # Select the first 15 columns for the final output
            final_columns = df.iloc[:, :15]

            # Append the processed DataFrame to the list
            combined_df_list.append(final_columns)

    # Combine all the DataFrames in the list into one DataFrame
    combined_df = pd.concat(combined_df_list, ignore_index=True)

    # Create a final output CSV file
    output_file_name = location_code + date_input + "_combined_output.csv"
    combined_df.to_csv(output_file_name, index=False)

    print(f"Combined CSV saved as: {output_file_name}")
