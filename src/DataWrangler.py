import polars as pl
from numbers_parser import Document
import warnings
import os


class DataWrangler:
    def __init__(self, config):
        self.config = config

    def _process_sheet_data(self, data, sheet_name):
        """Helper method to process data from a sheet and create a DataFrame"""
        if not data:
            raise ValueError(f"No data found in the {sheet_name} sheet")

        # Extract and validate column headers
        columns = data[0]
        if not columns or any(col is None for col in columns):
            raise ValueError(f"Invalid or missing column headers in {sheet_name} sheet")
        print(f"Found columns in {sheet_name}: {', '.join(columns)}")

        # Extract and validate data rows
        rows = data[1:]
        if not rows:
            raise ValueError(f"No data rows found in {sheet_name} sheet")

        # Filter out rows with None values and validate row length
        valid_rows = []
        skipped_rows = 0
        for row in rows:
            if (
                row is None
                or len(row) != len(columns)
                or any(cell is None for cell in row)
            ):
                skipped_rows += 1
                continue
            valid_rows.append(row)

        if skipped_rows > 0:
            print(f"Skipped {skipped_rows} invalid rows in {sheet_name}")

        if not valid_rows:
            raise ValueError(
                f"No valid data rows after filtering in {sheet_name} sheet"
            )

        # Create Polars DataFrame with validated data
        df = pl.DataFrame(data=valid_rows, schema=columns)

        # Drop any remaining null values
        df = df.drop_nulls()

        return df

    def load_updated_file(self):
        """Load and process both expenses and income sheets from the Numbers file"""
        # Suppress the specific RuntimeWarning about Numbers version
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", category=RuntimeWarning, message="unsupported version*"
            )

            try:
                # Check if the Numbers file exists
                numbers_path = self.config.get("libro_spese_path")
                if not os.path.exists(numbers_path):
                    raise FileNotFoundError(
                        f"Numbers file not found at: {numbers_path}"
                    )

                print(f"Loading Numbers file from: {numbers_path}")
                doc = Document(numbers_path)

                # Get all sheets
                sheets = doc.sheets
                if not sheets:
                    raise ValueError("No sheets found in the document")

                # Print all available sheet names for debugging
                print("Available sheets:", [sheet.name for sheet in sheets])

                # Dictionary to store DataFrames
                dfs = {}

                # Process each sheet we're interested in
                for sheet in sheets:
                    sheet_name = (
                        sheet.name.lower()
                    )  # Convert to lowercase for comparison
                    if sheet_name not in ["uscite", "entrate"]:
                        print(f"Skipping sheet: {sheet.name} (not a target sheet)")
                        continue

                    print(f"\nProcessing sheet: {sheet.name}")

                    # Get tables from the sheet
                    tables = sheet.tables
                    if not tables:
                        print(f"No tables found in {sheet.name} sheet, skipping...")
                        continue

                    # Get rows from the first table
                    data = tables[0].rows(values_only=True)

                    # Process the data and create DataFrame
                    df = self._process_sheet_data(data, sheet.name)

                    # Store DataFrame with standardized name
                    key_name = "spese" if sheet_name == "uscite" else sheet_name
                    dfs[key_name] = df

                    # Save to CSV
                    csv_path = self.config.get(f"finance_raw_{key_name}_path")
                    if csv_path:
                        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
                        df.write_csv(csv_path)
                        print(f"Saved {key_name} data to: {csv_path}")

                    print(f"Successfully processed {len(df)} rows from {sheet.name}")

                if not dfs:
                    raise ValueError(
                        "No valid sheets (uscite or entrate) found in the document"
                    )

                return dfs

            except Exception as e:
                print(f"Error processing Numbers file: {str(e)}")
                raise
