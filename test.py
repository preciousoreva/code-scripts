
#Creating a DataFrame from a dictionary 
#You can use Python with the pandas library to achieve this. Here's a step-by-step guide:
#1. Install pandas if you haven't already: `pip install pandas`

# 2. Import pandas in your Python script: `import pandas as pd`

# 3. Load your Excel sheet into a pandas DataFrame: `df_excel = pd.read_excel('your_excel_file.xlsx')`

# 4. Load the CSV file you want to match into another DataFrame: `df_csv = pd.read_csv('your_csv_file.csv')`

# 5. Identify the common columns between the two DataFrames that you want to use for matching. Let's say they are 'Product' and 'Date'.

# 6. Merge the two DataFrames based on these common columns: `df_merged = pd.merge(df_excel, df_csv, on=['Product', 'Date'])`

# 7. Now, you can modify the data in `df_merged` as needed. For example, you can update sales figures or transaction details.

# 8. Finally, save the modified data back to your Excel sheet or a new CSV file: `df_merged.to_excel('modified_excel_file.xlsx', index=False)` or `df_merged.to_csv('modified_csv_file.csv', index=False)`

# Here's a sample script:
import os
import pandas as pd
import math


current_dir = os.path.dirname(os.path.abspath(__file__))
update_folder_dir = os.path.join(current_dir, 'updates')


# Specify the directory where your CSV files are stored
directory = current_dir

# Get a list of all files in the directory
all_files = os.listdir(directory)

# Filter the list to only include CSV files
csv_files = [file for file in all_files if file.endswith('.csv')]

csv_count = len(csv_files)
print(f"Number of CSV files: {csv_count}")


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

data_frames = []

# Loop through each CSV file
for csv_file in csv_files:

    # Strip out the extension using os.path.splitext
    file_name_without_ext = os.path.splitext(csv_file)[0]

    # Split the remaining string by the hyphen ('-')
    parts = file_name_without_ext.split('-')

    # Join the remaining parts to form the date
    date_input = '-'.join(parts[1:])

    # Construct the full file path
    file_path = os.path.join(directory, csv_file)
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)


    # #ask user to imput data
    # date_input = input("Enter the date (DD-MM-YYYY): ")

    # #as user to enter location code
    location_code = parts[0]

    file_name = location_code + date_input

    #validate the location code
    if location_code.upper() not in location_dict:
        print("Invalid location code, please try again")
    else:
        #review item_tax code 
        df['*InvoiceNo'] = location_code.upper() + date_input
        df['*Customer'] = 'Walk In Customer'
        df['*InvoiceDate'] = date_input
        df['*DueDate'] = date_input
        df['Terms'] = 'Due on receipt'
        df['Location'] = location_dict[location_code]
        df['Memo'] = ''
        df['Item(Product/Service)'] = df['Name']
        df['ItemDescription'] = df['Description']
        df['ItemQuantity'] = df['Qty']
        df['ItemRate'] = ''
        df['*ItemAmount'] = df['ValueIncVAT']
        df['*ItemTaxCode'] = df['Name'].apply(lambda x: 'No VAT' if 'delivery' in x.lower() or 'pack' in x.lower() else 'Sales Tax')
        df['*ItemTaxAmount'] = ''
        df['Service Date'] = date_input

        # #Add the new column to the first position 
        # df.insert(0, 'Date', date_input)

        # remove the last row 
        df = df.drop(df.index[-1])

        # get to 14 
        df_first_14_columns = df.iloc[:, :15] 

        start_col = '*InvoiceNo'
        end_col = 'Service Date'
        final_columns = df.loc[:, start_col:end_col]

        # final_columns = df.iloc[:, 14:30]

        data_frames.append(final_columns)

        # # # create a new CSV 
        # final_columns.to_csv(f"{file_name}.csv", index=False)

# Concatenate all DataFrames into a single DataFrame
combined_data = pd.concat(data_frames, ignore_index=True)
    
DataDirectory = update_folder_dir  # Specify the directory where you want to save the file

output_file_name = 'combined_data'
# Construct the full path including the file name
# final_file_path = os.path.join(DataDirectory, f"{output_file_name}.csv")

# Check if the number of rows is more than or equal to 1000
row_count = len(combined_data)

# Define the chunk size (number of rows per file)
chunk_size = 1000

# Calculate the total number of chunks/files required
num_files = math.ceil(row_count / chunk_size)

for i in range(num_files):
    # Define the start and end row indices for this chunk
    start_row = i * chunk_size
    end_row = min((i + 1) * chunk_size, row_count)  # Ensure we don't exceed the total rows
    
    # Extract the chunk from the DataFrame
    chunk = combined_data.iloc[start_row:end_row, :]
    
    # Construct the output file path
    output_file_path = os.path.join(DataDirectory, f'{output_file_name}_part{i + 1}.csv')
    
    # Save the chunk to a CSV file
    chunk.to_csv(output_file_path, index=False)
    
    print(f"Data saved to {output_file_path}")


# # Save the DataFrame to the specified directory
# combined_data.to_csv(final_file_path, index=False)


print('here to test data')