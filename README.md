# FinancePlayground

A personal finance management application with an interactive dashboard to visualize expenses and income over time.

![Finance Dashboard](https://via.placeholder.com/800x450?text=Finance+Dashboard+Screenshot)

## Features

- **Data Import**: Automatically extract data from Apple Numbers spreadsheets
- **Interactive Dashboard**: Visualize your income and expenses with an intuitive dashboard
- **Category Analysis**: Break down expenses and income by category
- **Time Series Analysis**: Track your financial trends over time
- **Responsive Design**: Works on desktop and mobile devices

## Project Structure

```
FinancePlayground/
├── input/                   # Storage for imported data
├── src/                     # Source code
│   ├── Config.py            # Configuration management
│   ├── DataWrangler.py      # Data import and processing
│   ├── FinanceDashboard.py  # Dashboard visualization
│   ├── Process.py           # Data transformation
│   └── config.yaml          # Configuration file
├── tests/                   # Unit tests
├── .gitignore               # Git ignore file
├── .pre-commit-config.yaml  # Pre-commit hooks configuration
├── main.py                  # Application entry point
└── README.md                # Project documentation
```

## Prerequisites

- Python 3.8 or higher
- pip (Python package installer)
- Apple Numbers (for spreadsheet import functionality)

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/FinancePlayground.git
cd FinancePlayground
```

2. Create a virtual environment:

```bash
python -m venv venv
```

3. Activate the virtual environment:

   - On Windows:
   ```bash
   venv\Scripts\activate
   ```

   - On macOS/Linux:
   ```bash
   source venv/bin/activate
   ```

4. Install the dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

Before running the application, you need to update the configuration in `src/config.yaml`:

```yaml
libro_spese_path: "/path/to/your/Numbers/Document/Libro spese.numbers"
finance_raw_spese_path: "input/finance_raw_spese.csv"
finance_raw_entrate_path: "input/finance_raw_entrate.csv"
```

Update the `libro_spese_path` with the correct path to your Numbers spreadsheet. The format of the Numbers spreadsheet should include two sheets:

- **uscite**: For expenses with columns for Date, Description, Category, and Value
- **entrate**: For income with the same column structure

## Running the Application

To start the application, run:

```bash
python main.py
```

This will:
1. Load data from your Numbers spreadsheet
2. Process the data
3. Launch the dashboard at http://127.0.0.1:8050/

## Dashboard Usage

Once the dashboard is running, you can:

- Select a date range to filter the data
- View total income, expenses, and balance for the selected period
- Analyze spending patterns by category
- Track monthly financial trends

## Development

### Running Tests

To run the unit tests:

```bash
pytest tests/
```

### Code Formatting

The project uses Black for code formatting. To format your code before committing:

```bash
black .
```

A pre-commit hook is configured to automatically format your code when committing.

## Dependencies

- polars: For data processing
- dash: For the web dashboard
- plotly: For data visualization
- dash-bootstrap-components: For responsive UI components
- numbers-parser: For parsing Apple Numbers files
- pandera: For data validation
- pytest: For testing
- hypothesis: For property-based testing

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Acknowledgements

- [Dash](https://dash.plotly.com/) - For the dashboard framework
- [Plotly](https://plotly.com/python/) - For interactive visualizations
- [Bootstrap](https://getbootstrap.com/) - For responsive design components