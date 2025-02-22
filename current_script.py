import os
import pandas as pd
import math

# Directory and location setup
current_dir = os.path.dirname(os.path.abspath(__file__))
update_folder_dir = os.path.join(current_dir, 'updates')
os.makedirs(update_folder_dir, exist_ok=True)

# Location dictionary
location_dict = {
    'A': 'Talea Mall Rest (Chevron)',
    'B': '1004 (VI)',
    'C': 'Ayangbure (Main)',
    'D': 'Ayangbure Stand',
    'E': 'Dream Park (Main)',
    'F': 'Dream Park Stand',
    'G': 'Club (Ayangbure)',
    'H': 'Hotel (Ayangbure)',
    'L': 'Lounge (Ayangbure)',
    'M': 'Bask Lounge (Chevron)',
    'N': 'Shawarma Stand (Chevron)',
    'O': 'Pastry Pay Point (Chevron)'
}

# Fetch all CSV files
csv_files = [file for file in os.listdir(current_dir) if file.endswith('.csv')]

data_frames = []

# Process each CSV file
for csv_file in csv_files:
    file_name_without_ext = os.path.splitext(csv_file)[0]
    parts = file_name_without_ext.split('-')
    date_input = '-'.join(parts[1:])
    location_code = parts[0]
    
    file_path = os.path.join(current_dir, csv_file)
    df = pd.read_csv(file_path)

    if location_code.upper() not in location_dict:
        print(f"Invalid location code '{location_code}'. Skipping file '{csv_file}'.")
        continue

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
    df['*ItemTaxCode'] = df['Name'].apply(
        lambda x: 'No VAT' if 'delivery' in x.lower() or 'pack' in x.lower() else 'Sales Tax'
    )
    df['*ItemTaxAmount'] = ''
    df['Service Date'] = date_input

    df = df.drop(df.index[-1])  # Remove the last row if needed
    df_first_14_columns = df.iloc[:, :15]
    start_col = '*InvoiceNo'
    end_col = 'Service Date'
    final_columns = df.loc[:, start_col:end_col]
    data_frames.append(final_columns)

# Combine all DataFrames
combined_data = pd.concat(data_frames, ignore_index=True)

# Split by invoice number and save files
grouped = combined_data.groupby('*InvoiceNo')
current_file_rows = 0
current_chunk = []
file_index = 1
chunk_size = 1000

for _, group in grouped:
    group_rows = len(group)
    if current_file_rows + group_rows > chunk_size:
        # Save current chunk
        chunk_df = pd.concat(current_chunk, ignore_index=True)
        output_file_path = os.path.join(update_folder_dir, f'combined_data_part{file_index}.csv')
        chunk_df.to_csv(output_file_path, index=False)
        print(f"Data saved to {output_file_path}")
        current_chunk = []
        current_file_rows = 0
        file_index += 1
    current_chunk.append(group)
    current_file_rows += group_rows

# Save any remaining data
if current_chunk:
    chunk_df = pd.concat(current_chunk, ignore_index=True)
    output_file_path = os.path.join(update_folder_dir, f'combined_data_part{file_index}.csv')
    chunk_df.to_csv(output_file_path, index=False)
    print(f"Data saved to {output_file_path}")

print("Processing complete!")
