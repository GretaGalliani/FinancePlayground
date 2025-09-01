#!/filepath: src/dashboard/layouts.py
"""
DashboardLayout module for financial dashboard.

This module handles the layout and structure of the dashboard,
including the placement of components and styling of the UI.
"""

from datetime import datetime
from typing import Any, Dict

import dash_bootstrap_components as dbc
from dash import dcc, html


class DashboardLayout:
    """
    Handles the layout and structure of the dashboard.

    This class is responsible for setting up the layout of the dashboard,
    including the placement of components and styling of the UI.
    """

    def __init__(
        self,
        color_theme: Dict[str, Any],
        min_date: datetime,
        max_date: datetime,
        fonts: Dict[str, str],
        date_format: str,
    ):
        """
        Initialize the dashboard layout.

        Args:
            color_theme: Color theme for the dashboard
            min_date: Minimum date for the date picker
            max_date: Maximum date for the date picker
            fonts: Font configuration
            date_format: Format for date display
        """
        self.color_theme = color_theme
        self.min_date = min_date
        self.max_date = max_date
        self.fonts = fonts
        self.date_format = date_format

    def create_layout(self) -> Any:
        """
        Create the dashboard layout with bold purple headings.

        Returns:
            dbc.Container: Container with dashboard layout
        """
        # Calculate default date range (current month)
        today = datetime.now()
        current_month_start = datetime(today.year, today.month, 1)

        # Set default end date to current month
        end_date = current_month_start

        # Set default start date to 6 months before current month, or min_date if more recent
        start_date = datetime(
            today.year - 1 if today.month < 7 else today.year,
            today.month + 6 if today.month < 7 else today.month - 6,
            1,
        )

        # Make sure dates are within range
        if start_date < self.min_date:
            start_date = self.min_date
        if end_date > self.max_date:
            end_date = datetime(self.max_date.year, self.max_date.month, 1)

        title_font = self.fonts.get("title_font", "Montserrat")
        body_font = self.fonts.get("body_font", "Open Sans")

        # Apply styles directly to components
        body_style = {
            "backgroundColor": self.color_theme["background"],
            "fontFamily": f'"{body_font}", sans-serif',
            "color": self.color_theme.get("text", "#232323"),  # Dark text for body
        }

        heading_style = {
            "fontFamily": f'"{title_font}", sans-serif',
            "fontWeight": "700",  # Make bolder (700 instead of 600)
            "color": self.color_theme.get("headline", "#6C3BCE"),  # Purple for headings
            "marginBottom": "1.5rem",
        }

        tab_style = {
            "backgroundColor": self.color_theme["background"],
            "borderBottom": f"1px solid #E2E8F0",
            "padding": "12px 24px",
            "fontWeight": "600",
            "color": self.color_theme.get(
                "text", "#232323"
            ),  # Dark text for inactive tabs
        }

        active_tab_style = {
            "borderBottom": f"3px solid {self.color_theme.get('headline', '#6C3BCE')}",
            "fontWeight": "700",  # Make active tab bolder
            "color": self.color_theme.get(
                "headline", "#6C3BCE"
            ),  # Purple for active tab
        }

        # Dropdown styles consistent with dashboard design
        dropdown_style = {
            "width": "100%",
            "fontFamily": f'"{body_font}", sans-serif',
            "borderRadius": "4px",
            "boxShadow": "0 1px 3px rgba(0,0,0,0.1)",
        }

        # Label styles
        label_style = {
            "fontFamily": f'"{title_font}", sans-serif',
            "fontWeight": "600",
            "color": self.color_theme.get("headline", "#6C3BCE"),
            "marginBottom": "8px",
            "textAlign": "center",
        }

        # Create dropdown options for month selection in reverse chronological order
        current_date = self.max_date
        month_options = []

        while current_date >= self.min_date:
            # Format as YYYY-MM-01 for internal value and MMM YYYY for display
            first_of_month = datetime(current_date.year, current_date.month, 1)
            month_str = first_of_month.strftime("%Y-%m-%d")
            display_str = first_of_month.strftime("%b %Y")
            month_options.append({"label": display_str, "value": month_str})

            # Move to previous month
            if current_date.month == 1:
                current_date = datetime(current_date.year - 1, 12, 1)
            else:
                current_date = datetime(current_date.year, current_date.month - 1, 1)

        return dbc.Container(
            [
                html.H1(
                    "Personal Finance Dashboard",
                    className="text-center mt-4 mb-4",
                    style=heading_style,
                ),
                # Centered dropdown selectors with consistent styling
                dbc.Row(
                    [
                        dbc.Col(
                            html.Div(
                                [
                                    html.Label("Date Range", style=label_style),
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                [
                                                    html.Label(
                                                        "From:", className="mr-2 mt-2"
                                                    ),
                                                    dcc.Dropdown(
                                                        id="start-month-dropdown",
                                                        options=month_options,
                                                        value=start_date.strftime(
                                                            "%Y-%m-%d"
                                                        ),
                                                        clearable=False,
                                                        style=dropdown_style,
                                                    ),
                                                ],
                                                width=5,
                                                className="pr-0",
                                            ),
                                            dbc.Col(
                                                html.Div(
                                                    html.I(
                                                        className="fas fa-arrow-right"
                                                    ),
                                                    className="d-flex align-items-center justify-content-center h-100",
                                                    style={"marginTop": "15px"},
                                                ),
                                                width=2,
                                                className="px-0 text-center",
                                            ),
                                            dbc.Col(
                                                [
                                                    html.Label(
                                                        "To:", className="mr-2 mt-2"
                                                    ),
                                                    dcc.Dropdown(
                                                        id="end-month-dropdown",
                                                        options=month_options,
                                                        value=end_date.strftime(
                                                            "%Y-%m-%d"
                                                        ),
                                                        clearable=False,
                                                        style=dropdown_style,
                                                    ),
                                                ],
                                                width=5,
                                                className="pl-0",
                                            ),
                                        ],
                                        className="align-items-end",
                                    ),
                                ],
                                className="p-3 mb-3 shadow-sm rounded",
                                style={
                                    "backgroundColor": "#FFFFFF",
                                    "border": f"1px solid {self.color_theme.get('headline', '#6C3BCE')}",
                                },
                            ),
                            width={"size": 6, "offset": 3},
                            className="text-center",
                        )
                    ],
                    className="mb-4",
                ),
                # Unified summary cards row with all 4 cards
                html.Div(id="summary-cards", className="mb-4 mt-4"),
                dbc.Tabs(
                    [
                        dbc.Tab(
                            [
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            dcc.Graph(id="main-dashboard"),
                                            width=12,
                                            className="mb-4",
                                        ),
                                    ]
                                ),
                                # REORDERED: Display stacked charts (monthly breakdown) in full rows
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            dcc.Graph(
                                                id="stacked-expenses",
                                                style={
                                                    "height": "500px"
                                                },  # Taller graph
                                            ),
                                            width=12,
                                            className="mb-4",
                                        ),
                                    ]
                                ),
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            dcc.Graph(
                                                id="stacked-income",
                                                style={
                                                    "height": "500px"
                                                },  # Taller graph
                                            ),
                                            width=12,
                                            className="mb-4",
                                        ),
                                    ]
                                ),
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            dcc.Graph(id="expense-pie-chart"),
                                            width=6,
                                            className="mb-4",
                                        ),
                                        dbc.Col(
                                            dcc.Graph(id="income-pie-chart"),
                                            width=6,
                                            className="mb-4",
                                        ),
                                    ]
                                ),
                            ],
                            label="Expenses & Income",
                            tab_id="expenses-tab",
                            style=tab_style,
                            active_tab_style=active_tab_style,
                        ),
                        dbc.Tab(
                            [
                                # Removed savings cards since they're now in the main summary cards
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            dcc.Graph(id="savings-overview"), width=8
                                        ),
                                        dbc.Col(
                                            dcc.Graph(id="savings-breakdown"), width=4
                                        ),
                                    ],
                                    className="mb-4",
                                ),
                                dcc.Graph(id="savings-allocation"),
                                html.H4(
                                    "Savings Transactions",
                                    className="mt-4 mb-3",
                                    style=heading_style,
                                ),
                                html.Div(id="savings-table"),
                            ],
                            label="Savings",
                            tab_id="savings-tab",
                            style=tab_style,
                            active_tab_style=active_tab_style,
                        ),
                    ],
                    id="dashboard-tabs",
                    active_tab="expenses-tab",
                    style={"backgroundColor": self.color_theme["background"]},
                ),
            ],
            fluid=True,
            style=body_style,
        )
