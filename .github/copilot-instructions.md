# Copilot Instructions for AI Coding Agents

## Project Overview
This codebase is a Python project focused on processing, transforming, and auditing CSV sales data, primarily using Flask for web-based file uploads and Pandas for data manipulation. The project is organized for rapid, script-driven workflows and includes custom logic for location-based sales processing and audit comparison.

## Major Components
- **Flask Web App (`app.py`)**: Handles CSV file uploads via a web interface (`templates/upload.html`). Uploaded files are processed and saved to the `uploads/` directory, then transformed and output to `updates/`.
- **CSV Processing Logic**: The filename format (`LOCATION-DATE.csv`) is critical for routing and transforming data. Location codes map to human-readable names. Data is cleaned, columns are renamed, and output is saved as `processed_<original>.csv` in `updates/`.
- **Audit Scripts (`audit-record/compare.py`)**: Contains logic for comparing sales data across sources, using hardcoded multiline strings and dictionary-based comparison. Useful for reconciling discrepancies between systems.

## Developer Workflows
- **Run the Flask App**: Execute `python app.py` (ensure the `myenv` virtual environment is activated).
- **Upload and Process Files**: Use the web interface at `/` to upload CSVs. Processed files are saved in `updates/`.
- **Audit/Compare Data**: Run `audit-record/compare.py` directly for sales data reconciliation.
- **Virtual Environment**: Activate with `source myenv/bin/activate` before running scripts.

## Project-Specific Conventions
- **Filename Format**: Uploaded CSVs must follow `LOCATION-DATE.csv` (e.g., `E31-01-2025.csv`). Location codes are mapped in `app.py`.
- **Column Mapping**: Data transformation in `app.py` uses explicit column renaming and value assignment. See `process_csv_files()` for details.
- **Error Handling**: User feedback is provided via Flask `flash()` messages, visible in the web UI.
- **Directory Structure**: All uploads go to `uploads/`, processed files to `updates/`. Audit scripts are in `audit-record/`.

## Integration Points & Dependencies
- **Flask**: Web server and routing.
- **Pandas**: Data manipulation and CSV I/O.
- **Jinja2**: HTML templating for upload UI.
- **No database integration**: All data is file-based.

## Examples
- **Uploading a file**: Use the web UI, ensure filename matches convention.
- **Processing logic**: See `process_csv_files()` in `app.py` for column mapping and output.
- **Audit comparison**: See `structure_sales_data()` and `compare_sales_values()` in `audit-record/compare.py`.

## Key Files & Directories
- `app.py`: Main Flask app and CSV processing logic
- `audit-record/compare.py`: Sales data audit/comparison
- `templates/upload.html`: File upload UI
- `uploads/`: Raw uploaded files
- `updates/`: Processed output files

## Quickstart
1. Activate the environment: `source myenv/bin/activate`
2. Run the app: `python app.py`
3. Upload a CSV via the web UI
4. Find processed files in `updates/`
5. Run audit scripts as needed

---
For questions or unclear conventions, review the referenced files or ask for clarification.
