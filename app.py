from flask import Flask, request, redirect, url_for, render_template, flash
import os
import pandas as pd

app = Flask(__name__)
app.secret_key = 'supersecretkey'
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = file.filename
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            flash('File successfully uploaded')
            process_file(file_path)
            return redirect(url_for('upload_file'))
    return render_template('upload.html')

def process_file(file_path):
    try:
        process_csv_files(file_path)
        return True
    except Exception as e:
        flash(f"Error in processing: {str(e)}")
        return False

def process_csv_files(file_path):
    # Directory and location setup
    current_dir = os.path.dirname(os.path.abspath(file_path))
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

    # Get the uploaded file's name without extension
    file_name = os.path.basename(file_path)
    file_name_without_ext = os.path.splitext(file_name)[0]
    parts = file_name_without_ext.split('-')
    
    # Check if the filename follows the expected format
    if len(parts) < 2:
        flash(f"Invalid file name format: {file_name}. Expected format: LOCATION-DATE.csv")
        return
    
    date_input = '-'.join(parts[1:])
    location_code = parts[0]
    
    if location_code.upper() not in location_dict:
        flash(f"Invalid location code '{location_code}' in file '{file_name}'.")
        return

    # Process the uploaded file
    try:
        df = pd.read_csv(file_path)
        
        df['*InvoiceNo'] = location_code.upper() + date_input
        df['*Customer'] = 'Walk In Customer'
        df['*InvoiceDate'] = date_input
        df['*DueDate'] = date_input
        df['Terms'] = 'Due on receipt'
        df['Location'] = location_dict[location_code.upper()]
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

        # Remove the last row if it's a total
        df = df.drop(df.index[-1])
        
        # Select only the required columns
        start_col = '*InvoiceNo'
        end_col = 'Service Date'
        final_columns = df.loc[:, start_col:end_col]
        
        # Save the processed file
        output_file_path = os.path.join(update_folder_dir, f'processed_{file_name}')
        final_columns.to_csv(output_file_path, index=False)
        flash(f"File processed successfully. Saved to {output_file_path}")
        
    except Exception as e:
        flash(f"Error processing file: {str(e)}")

if __name__ == '__main__':
    app.run(debug=True)
