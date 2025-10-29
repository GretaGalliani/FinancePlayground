"""
Dashboard components module for the Finance Dashboard.

This module contains reusable components used throughout the dashboard,
including configuration dataclasses, card creators, and utility classes
for data formatting and display.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Union

import dash_bootstrap_components as dbc
import polars as pl
from dash import dash_table, html


@dataclass
class DashboardConfig:
    """Configuration container for the dashboard."""

    color_theme: Dict[str, Any]
    chart_styling: Dict[str, Any] = field(default_factory=dict)
    fonts: Dict[str, str] = field(default_factory=dict)
    date_display_format: str = "DD/MM/YYYY"


class CardCreator:
    """
    Creates summary cards for the dashboard.

    This class is responsible for generating summary cards for income,
    expenses, balance, and savings metrics.
    """

    def __init__(self, color_theme: Dict[str, Any]):
        """
        Initialize the card creator.

        Args:
            color_theme: Color theme for the cards
        """
        self.color_theme = color_theme

    def create_summary_cards(
        self,
        df_monthly_summary: Optional[pl.DataFrame],
        df_savings_metrics: Optional[pl.DataFrame],
    ) -> Any:
        """
        Create summary cards for income, expenses, balance, and savings with improved formatting.

        Args:
            df_monthly_summary: DataFrame with monthly summary data
            df_savings_metrics: DataFrame with savings metrics data

        Returns:
            dbc.Row: Row of cards for the dashboard
        """
        if df_monthly_summary is None or len(df_monthly_summary) == 0:
            return html.Div(
                "No data available for the selected period.",
                style={"color": self.color_theme.get("headline", "#6C3BCE")},
                className="text-center p-4",
            )

        # Calculate income, expenses, and balance metrics
        total_income = (
            df_monthly_summary["Income"].sum()
            if "Income" in df_monthly_summary.columns
            else 0
        )
        total_expenses = (
            df_monthly_summary["Expenses"].sum()
            if "Expenses" in df_monthly_summary.columns
            else 0
        )
        total_balance = total_income - total_expenses

        months_count = len(df_monthly_summary)
        monthly_avg_income = total_income / months_count if months_count > 0 else 0
        monthly_avg_expenses = total_expenses / months_count if months_count > 0 else 0
        monthly_avg_balance = total_balance / months_count if months_count > 0 else 0

        # Calculate total savings - with more robust error handling
        total_savings = 0.0
        latest_month = "N/A"

        if (
            df_savings_metrics is not None
            and len(df_savings_metrics) > 0
            and "Month" in df_savings_metrics.columns
        ):
            try:
                # Get the latest month's metrics
                latest_month_raw = df_savings_metrics["Month"].max()
                latest_metrics = df_savings_metrics.filter(
                    pl.col("Month") == latest_month_raw
                )

                if len(latest_metrics) > 0 and "TotalSavings" in latest_metrics.columns:
                    total_savings = latest_metrics["TotalSavings"][0]

                # Format month as "Month Year" (e.g., "October 2025")
                from datetime import datetime

                month_date = datetime.strptime(f"{latest_month_raw}-01", "%Y-%m-%d")
                latest_month = month_date.strftime("%B %Y")
            except Exception as e:
                # Handle any unexpected errors silently
                pass

        # Format numbers with thousands separator (,) and decimal point (.)
        def format_currency(value):
            return f"{value:,.2f}€"

        cards = dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Total Income", className="card-title"),
                                html.H3(
                                    format_currency(total_income),
                                    className="card-text font-weight-bold",
                                    style={
                                        "color": self.color_theme.get(
                                            "income", "#078080"
                                        )
                                    },
                                ),
                                html.P(
                                    f"Monthly average: {format_currency(monthly_avg_income)}",
                                    className="text-muted small",
                                ),
                            ]
                        ),
                        className="shadow-sm",
                        style={
                            "background-color": "#fffffe",
                            "border-color": self.color_theme.get("income", "#078080"),
                        },
                    ),
                    width=3,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Total Expenses", className="card-title"),
                                html.H3(
                                    format_currency(total_expenses),
                                    className="card-text font-weight-bold",
                                    style={
                                        "color": self.color_theme.get(
                                            "expense", "#F45D48"
                                        )
                                    },
                                ),
                                html.P(
                                    f"Monthly average: {format_currency(monthly_avg_expenses)}",
                                    className="text-muted small",
                                ),
                            ]
                        ),
                        className="shadow-sm",
                        style={
                            "background-color": "#fffffe",
                            "border-color": self.color_theme.get("expense", "#F45D48"),
                        },
                    ),
                    width=3,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Balance", className="card-title"),
                                html.H3(
                                    format_currency(total_balance),
                                    className="card-text font-weight-bold",
                                    style={
                                        "color": self.color_theme.get(
                                            "balance", "#4361EE"
                                        )
                                    },
                                ),
                                html.P(
                                    f"Monthly average: {format_currency(monthly_avg_balance)}",
                                    className="text-muted small",
                                ),
                            ]
                        ),
                        className="shadow-sm",
                        style={
                            "background-color": "#fffffe",
                            "border-color": self.color_theme.get("balance", "#4361EE"),
                        },
                    ),
                    width=3,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Total Savings", className="card-title"),
                                html.H3(
                                    format_currency(total_savings),
                                    className="card-text font-weight-bold",
                                    style={
                                        "color": self.color_theme.get(
                                            "savings", {}
                                        ).get("total", "#6C3BCE")
                                    },
                                ),
                                html.P(
                                    f"As of {latest_month}",
                                    className="text-muted small",
                                ),
                            ]
                        ),
                        className="shadow-sm",
                        style={
                            "background-color": "#fffffe",
                            "border-color": self.color_theme.get("savings", {}).get(
                                "total", "#6C3BCE"
                            ),
                        },
                    ),
                    width=3,
                ),
            ],
            className="mb-4",
        )

        return cards

    def create_savings_table(self, df_savings: Optional[pl.DataFrame]) -> Any:
        """
        Create a table of savings transactions with white background.

        Args:
            df_savings: DataFrame with savings transactions

        Returns:
            dash_table.DataTable: DataTable component for the dashboard
        """
        if df_savings is None or len(df_savings) == 0:
            return html.Div(
                "No savings transactions available for the selected period.",
                style={"color": self.color_theme.get("headline", "#6C3BCE")},
                className="text-center p-4",
            )

        # Create a copy of the dataframe with sorted data
        df_table = df_savings.sort("Date", descending=True)

        # Format date and numeric columns for display
        df_table = df_table.with_columns(
            [
                pl.col("Date").dt.strftime("%d/%m/%Y").alias("Date"),
                pl.col("Value")
                .map_elements(lambda x: f"€{x:.2f}", return_dtype=pl.Utf8)
                .alias("Amount"),
            ]
        )

        # Select and rename columns for display
        df_display = df_table.select(
            ["Date", "Description", "Category", "CategoryType", "Value"]
        )

        # Convert directly to records for Dash without using pandas
        records = df_display.to_dicts()

        # Create the table with white background and purple accents
        table = dash_table.DataTable(
            data=records,
            columns=[{"name": col, "id": col} for col in df_display.columns],
            style_table={"overflowX": "auto"},
            style_cell={
                "textAlign": "left",
                "padding": "10px",
                "whiteSpace": "normal",
                "height": "auto",
                "backgroundColor": "white",
            },
            style_header={
                "backgroundColor": "rgba(108, 59, 206, 0.1)",  # Light purple background
                "fontWeight": "bold",
                "color": self.color_theme.get("headline", "#6C3BCE"),
                "borderBottom": f"2px solid {self.color_theme.get('headline', '#6C3BCE')}",
            },
            style_data_conditional=[
                {
                    "if": {"filter_query": '{CategoryType} = "Accantonamento"'},
                    "backgroundColor": "rgba(7, 128, 128, 0.05)",  # Very light teal
                    "borderLeft": f"3px solid {self.color_theme['income']}",
                },
                {
                    "if": {"filter_query": "{Value} < 0"},
                    "backgroundColor": "rgba(244, 93, 72, 0.05)",  # Very light coral
                    "borderLeft": f"3px solid {self.color_theme['expense']}",
                },
            ],
            page_size=10,
        )

        return table


class DateParser:
    """
    Handles parsing and standardization of dates in various formats.

    This class provides utilities for parsing dates from strings in
    multiple formats and handling edge cases.
    """

    @staticmethod
    def parse_date(date_string: Union[str, datetime]) -> datetime:
        """
        Parse date string in multiple formats to handle different possible inputs.

        Args:
            date_string: String or datetime representation of date to parse

        Returns:
            datetime: Parsed datetime object

        Raises:
            ValueError: If the date cannot be parsed
        """
        if isinstance(date_string, datetime):
            return date_string

        # Try multiple date formats
        formats = [
            "%Y-%m-%dT%H:%M:%S",  # ISO format with time
            "%Y-%m-%d",  # ISO date
            "%d/%m/%Y",  # European date
            "%Y-%m-%d %H:%M:%S",  # ISO datetime
        ]

        for fmt in formats:
            try:
                # Handle possible milliseconds in ISO format
                if isinstance(date_string, str) and "." in date_string:
                    date_string = date_string.split(".")[0]
                return datetime.strptime(date_string, fmt)
            except (ValueError, AttributeError):
                continue

        # If we get here, none of the formats worked
        raise ValueError(f"Could not parse date: {date_string}")
