# FinancePlayground

A personal finance management application with an interactive dashboard to visualize expenses, income, and savings over time. The application automatically extracts data from Apple Numbers spreadsheets and provides comprehensive financial analytics through an intuitive web interface.

> **Note**: This application is specifically tailored to my personal finance tracking system with Italian categories and a particular data structure. While the code is designed for my specific use case, it may serve as inspiration or a starting point for others looking to build their own finance tracking solutions. Feel free to adapt the categories, data structure, and workflows to match your own financial management needs.

## Features

- **Data Import**: Automatically extract data from Apple Numbers spreadsheets with three sheets (uscite, entrate, risparmi)
- **Interactive Dashboard**: Visualize your income, expenses, and savings with an intuitive web-based dashboard
- **Category Analysis**: Break down expenses, income, and savings by category with consistent color coding
- **Time Series Analysis**: Track your financial trends over time with monthly summaries and stacked visualizations
- **Savings Tracking**: Comprehensive savings management with allocation tracking and category breakdown
- **Error Handling**: Comprehensive error handling with detailed logging and skipped rows reporting

## Project Structure

```
FinancePlayground/
├── input/                          # Storage for imported and processed data
│   ├── finance_raw_expenses.csv    # Raw expense data from Numbers
│   ├── finance_raw_income.csv      # Raw income data from Numbers
│   ├── finance_raw_savings.csv     # Raw savings data from Numbers
│   ├── finance_processed_*.csv     # Processed data files
│   └── finance_monthly_savings.csv # Monthly savings summary
├── output/                         # Generated visualization datasets
│   ├── monthly_summary.csv         # Monthly income/expense summary
│   ├── expenses_by_category.csv    # Expense breakdown by category
│   ├── income_by_category.csv      # Income breakdown by category
│   ├── savings_metrics.csv         # Savings metrics over time
│   └── *.csv                       # Other analytical datasets
├── logs/                           # Application logs
├── src/                            # Source code
│   ├── config.py                   # Configuration management
│   ├── config.yaml                 # Configuration file
│   ├── data_wrangler.py           # Numbers file processing and data extraction
│   ├── process.py                  # Data transformation and standardization
│   ├── finance_dashboard.py        # Dashboard visualization and web interface
│   ├── category_mapper.py          # Consistent category color mapping
│   ├── models.py                   # Pydantic data models and validation
│   ├── logger.py                   # Logging utilities
│   └── main.py                     # Application orchestration and entry point
├── tests/                          # Unit tests
│   ├── test_config.py              # Configuration tests
│   ├── test_logger.py              # Logger tests
│   └── test_models.py              # Model validation tests
├── .gitignore                      # Git ignore file
├── .pre-commit-config.yaml         # Pre-commit hooks configuration
├── environment.yml                 # Conda environment specification
└── README.md                       # Project documentation
```

## Prerequisites

- Python 3.9 or higher
- Conda or pip for package management
- Apple Numbers (for spreadsheet data source)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/FinancePlayground.git
cd FinancePlayground
```

1. Create and activate the conda environment:
```bash
conda env create -f environment.yml
conda activate FinancePlayground
```

## Configuration

Update the configuration in `src/config.yaml` to match your setup:

```yaml
# Path to your Numbers spreadsheet
numbers_file_path: "/path/to/your/Numbers/Document/Libro spese.numbers"

# Sheet mappings (Italian to English)
numbers_sheet_mappings:
  uscite: "expenses"    # Expense sheet
  entrate: "income"     # Income sheet
  risparmi: "savings"   # Savings sheet
```

### Numbers Spreadsheet Format

Your Numbers document should contain three sheets with the following structure:

#### Uscite (Expenses) Sheet
- **Data**: Date (DD/MM/YY format)
- **Descrizione**: Transaction description
- **Categoria**: Expense category
- **Importo**: Amount (negative for expenses)

#### Entrate (Income) Sheet
- **Data**: Date (DD/MM/YY format)
- **Descrizione**: Transaction description
- **Categoria**: Income category
- **Importo**: Amount (positive for income)

#### Risparmi (Savings) Sheet
- **Data**: Date (DD/MM/YY format)
- **Descrizione**: Transaction description
- **Categoria**: Savings category
- **Tipo categoria**: Category type ("Risparmio" or "Accantonamento")
- **Importo**: Amount (positive for savings/allocations, negative for withdrawals)

### Valid Categories

The application supports predefined categories that can be customized in `config.yaml`:

**Expense Categories:**
- Altro, Benessere Personale, Cibo, Formazione, Imposte, Oggetti personali, Servizi, Spese mediche, Svago, Veicoli, Viaggi

**Income Categories:**
- Altro, Bonus, Extra, Finanza, Rimborso tasse, Stipendio, Welfare

**Savings Categories:**
- Accantomenti generali, Risparmi generali, Fondo vacanze, Psicologa

## Running the Application

### Quick Start

```bash
cd src
python main.py
```

### What Happens When You Run

1. **Data Loading**: Loads data from your Numbers spreadsheet or cached CSV files
2. **Data Processing**: Validates and standardizes all financial data
3. **Dataset Generation**: Creates analytical datasets for visualization
4. **Dashboard Launch**: Starts the web server at http://127.0.0.1:8050/

## Dashboard Features

### Main Dashboard Tab (Expenses & Income)
- **Summary Cards**: Total income, expenses, balance, and savings overview
- **Monthly Overview**: Combined bar and line chart showing income, expenses, and balance trends
- **Monthly Breakdowns**: Stacked bar charts showing category composition over time
- **Category Breakdown**: Donut charts for expense and income categories

### Savings Tab
- **Savings Overview**: Area chart showing savings category balances over time
- **Category Breakdown**: Pie chart of current savings by category
- **Allocation Status**: Comparison of allocated vs spent funds
- **Transaction Table**: Detailed view of all savings transactions

### Interactive Features
- **Date Range Selection**: Filter all visualizations by month range
- **Consistent Color Coding**: Categories maintain the same colors across all charts
- **Unified Hover Information**: Comprehensive data display on chart interactions

## Development

### Code Style and Quality

The project uses several tools to maintain code quality:

```bash
# Format code with Black
black .

# Sort imports with isort
isort .

# Run pre-commit hooks
pre-commit run --all-files
```

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test modules
pytest tests/test_config.py
pytest tests/test_models.py
pytest tests/test_logger.py

# Run with coverage
pytest tests/ --cov=src
```

### Project Architecture

The application follows clean architecture principles with clear separation of concerns:

- **Config**: Centralized configuration management with YAML support
- **DataWrangler**: Numbers file parsing and raw data extraction
- **Process**: Data validation, transformation, and standardization
- **FinanceDashboard**: Web interface and visualization components
- **CategoryMapper**: Consistent color mapping for categories
- **Models**: Pydantic validation models and data structures

## Key Dependencies

### Core Libraries
- **polars**: High-performance data processing and analysis
- **dash**: Web dashboard framework with Bootstrap components
- **plotly**: Interactive data visualizations
- **numbers-parser**: Apple Numbers file parsing
- **pydantic**: Data validation and settings management
- **pandera**: Data schema validation

### Development Tools
- **pytest**: Testing framework with hypothesis for property-based testing
- **black**: Code formatting
- **isort**: Import sorting
- **pre-commit**: Git hooks for code quality

## Data Flow

1. **Extract**: DataWrangler reads Apple Numbers file and extracts raw data
2. **Transform**: Process module validates, cleans, and standardizes data formats
3. **Load**: Analytical datasets are generated and saved to output folder
4. **Visualize**: Dashboard loads datasets and creates interactive visualizations

## Error Handling and Logging

- **Comprehensive Logging**: All operations are logged with timestamps and context
- **Graceful Degradation**: Application continues with cached data if Numbers file is unavailable
- **Validation Reports**: Detailed reports of skipped rows and validation errors
- **Exception Handling**: Robust error handling with informative error messages

## Troubleshooting

### Common Issues

1. **Numbers File Not Found**: Verify the path in `config.yaml` is correct
2. **Missing Sheets**: Ensure your Numbers document has sheets named "uscite", "entrate", and "risparmi"
3. **Date Format Issues**: Check that dates are in DD/MM/YY format
4. **Category Validation**: Unknown categories are automatically assigned to "Altro" or "Varie"

### Log Files

Check the `logs/` directory for detailed error information and processing statistics.

## Configuration Reference

Key configuration sections in `config.yaml`:

```yaml
# Numbers file settings
numbers_file_path: "path/to/your/spreadsheet.numbers"
numbers_sheet_mappings:
  uscite: "expenses"
  entrate: "income"
  risparmi: "savings"

# Category validation
valid_expenses_categories: [...]
valid_income_categories: [...]
valid_savings_categories: [...]

# Color theming
color_palette:
  income: "#078080"
  expense: "#F45D48"
  balance: "#4361EE"

# Dashboard settings
dashboard_port: 8050
debug_mode: false
```
