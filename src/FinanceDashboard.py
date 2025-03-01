import polars as pl
from dash import dcc, html, Input, Output, dash_table
import plotly.graph_objects as go
import plotly.express as px
import dash_bootstrap_components as dbc
import dash
from datetime import datetime, timedelta


class FinanceDashboard:
    def __init__(self, df_spese, df_entrate, df_risparmi=None, df_savings_monthly=None):
        self.df_spese = df_spese.sort("Date")
        self.df_entrate = df_entrate.sort("Date")
        self.df_risparmi = df_risparmi
        self.df_savings_monthly = df_savings_monthly

        self.app = dash.Dash(
            __name__,
            external_stylesheets=[
                dbc.themes.BOOTSTRAP,
                "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css",
            ],
        )

        self.color_theme = {
            "income": "#2ecc71",  # Green for income
            "expense": "#e74c3c",  # Red for expenses
            "balance": "#3498db",  # Blue for balance/net
            "background": "#f8f9fa",  # Light background
            "savings": {
                "general": "#9b59b6",  # Purple for general savings
                "vacation": "#f1c40f",  # Yellow for vacation fund
                "therapy": "#1abc9c",  # Turquoise for therapy fund
                "misc": "#34495e",  # Dark blue for miscellaneous fund
                "total": "#8e44ad",  # Dark purple for total savings
                "allocation": "#e67e22",  # Orange for allocations
                "spent": "#27ae60",  # Green for spent
            },
            "categories": [
                "#e74c3c",
                "#3498db",
                "#9b59b6",
                "#f1c40f",
                "#2ecc71",
            ],  # Color palette for categories
        }

        self.setup_layout()
        self.setup_callbacks()

    def _calculate_monthly_breakdown(self, df, kind):
        """Aggregate monthly data by category."""
        breakdown = (
            df.with_columns(pl.col("Date").dt.strftime("%Y-%m").alias("Month"))
            .groupby(["Month", "Category"])
            .agg(pl.col("Value").sum().alias(kind))
            .sort(["Month", "Category"])
        )
        return breakdown

    def create_stacked_bar(self, df, kind, title):
        """Create a stacked bar plot for expenses or income."""
        unique_categories = df["Category"].unique().to_list()
        colors = self.color_theme["categories"] * (
            len(unique_categories) // len(self.color_theme["categories"]) + 1
        )

        fig = go.Figure()

        for category, color in zip(unique_categories, colors):
            filtered = df.filter(pl.col("Category") == category)
            fig.add_trace(
                go.Bar(
                    x=filtered["Month"].to_list(),
                    y=filtered[kind].to_list(),
                    name=category,
                    marker_color=color,
                )
            )

        fig.update_layout(
            title=title,
            barmode="stack",
            yaxis=dict(title="Amount (€)"),
            plot_bgcolor=self.color_theme["background"],
            showlegend=True,
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
            margin=dict(l=50, r=50, t=50, b=50),
            hovermode="x",
        )

        return fig

    def _calculate_monthly_summary(self, start_date, end_date):
        """Calculate monthly summary for income, expenses, and balance."""
        # Combine and filter the data
        combined = (
            self.df_spese.with_columns(pl.lit("Expense").alias("Type")).vstack(
                self.df_entrate.with_columns(pl.lit("Income").alias("Type"))
            )
        ).filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))

        # Group by month and type, and calculate the totals
        monthly_summary = (
            combined.with_columns(pl.col("Date").dt.strftime("%Y-%m").alias("Month"))
            .groupby(["Month", "Type"])
            .agg(pl.col("Value").sum().alias("Total"))
            .pivot(
                values="Total",
                index="Month",
                columns="Type",
                aggregate_function="first",
            )
            .with_columns((pl.col("Income") - pl.col("Expense")).alias("Balance"))
            .fill_null(0)  # Fill missing values with 0
            .sort("Month")
        )
        return monthly_summary

    def _filter_savings_data(self, start_date, end_date):
        """Filter savings data for the specified date range."""
        if self.df_risparmi is None:
            return None

        # Filter the raw savings data
        filtered_savings = self.df_risparmi.filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        return filtered_savings

    def _filter_savings_monthly_data(self, start_date, end_date):
        """Filter monthly savings data for the date range."""
        if self.df_savings_monthly is None:
            return None

        # Get the month range
        start_month = datetime.strftime(start_date, "%Y-%m")
        end_month = datetime.strftime(end_date, "%Y-%m")

        # Filter the monthly savings data
        filtered_monthly = self.df_savings_monthly.filter(
            (pl.col("Month") >= start_month) & (pl.col("Month") <= end_month)
        )

        return filtered_monthly

    def create_savings_figure(self, df_savings_monthly):
        """Create a figure showing savings trends."""
        if df_savings_monthly is None or len(df_savings_monthly) == 0:
            # Return an empty figure with a message if no data
            fig = go.Figure()
            fig.update_layout(
                title="Savings Overview - No Data Available",
                yaxis=dict(title="Amount (€)"),
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No savings data available for selected period",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        fig = go.Figure()

        # Get the unique categories
        unique_categories = df_savings_monthly["Category"].unique().to_list()
        unique_accounts = df_savings_monthly["Account"].unique().to_list()

        # Create traces for each account/category combination
        for account in unique_accounts:
            for category in unique_categories:
                filtered = df_savings_monthly.filter(
                    (pl.col("Account") == account)
                    & (pl.col("Category") == category)
                    & (
                        pl.col("AllocationType") == "Spent"
                    )  # Only show spent amounts for the trend
                )

                if len(filtered) > 0:
                    fig.add_trace(
                        go.Scatter(
                            x=filtered["Month"].to_list(),
                            y=filtered["TotalValue"].to_list(),
                            name=f"{account} - {category}",
                            mode="lines+markers",
                        )
                    )

        # Add total savings line with a thicker line if available
        total_savings = (
            df_savings_monthly.filter(pl.col("TotalSavings").is_not_null())
            .select(["Month", "TotalSavings"])
            .unique("Month")
        )

        if len(total_savings) > 0:
            fig.add_trace(
                go.Scatter(
                    x=total_savings["Month"].to_list(),
                    y=total_savings["TotalSavings"].to_list(),
                    name="Total Savings",
                    line=dict(color=self.color_theme["savings"]["total"], width=4),
                )
            )

        fig.update_layout(
            title="Savings Overview",
            yaxis=dict(title="Amount (€)"),
            plot_bgcolor=self.color_theme["background"],
            hovermode="x",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )

        return fig

    def create_savings_breakdown_figure(self, df_savings_monthly):
        """Create a figure showing savings breakdown as a pie chart."""
        if df_savings_monthly is None or len(df_savings_monthly) == 0:
            # Return an empty figure with a message if no data
            fig = go.Figure()
            fig.update_layout(
                title="Savings Breakdown - No Data Available",
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No savings data available for selected period",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Get the last month
        last_month = df_savings_monthly["Month"].max()

        # Filter to just spent (not allocations) for the latest month
        latest_data = df_savings_monthly.filter(
            (pl.col("Month") == last_month) & (pl.col("AllocationType") == "Spent")
        )

        # Calculate category totals
        category_totals = latest_data.groupby(["Category"]).agg(
            pl.col("TotalValue").sum().alias("Value")
        )

        # Create the pie chart using Plotly Graph Objects
        fig = go.Figure()

        # Extract categories and values for the pie chart
        categories = category_totals["Category"].to_list()
        values = category_totals["Value"].to_list()

        # Add pie trace
        fig.add_trace(
            go.Pie(
                labels=categories,
                values=values,
                hole=0.3,
                textinfo="label+percent",
                insidetextorientation="radial",
            )
        )

        fig.update_layout(
            title=f"Savings Breakdown - {last_month}",
            plot_bgcolor=self.color_theme["background"],
        )

        return fig

    def create_savings_allocation_figure(self, df_savings_monthly):
        """Create a figure comparing allocated vs spent savings."""
        if df_savings_monthly is None or len(df_savings_monthly) == 0:
            # Return an empty figure with a message if no data
            fig = go.Figure()
            fig.update_layout(
                title="Allocation Status - No Data Available",
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No savings data available for selected period",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Get the last month
        last_month = df_savings_monthly["Month"].max()

        # Get the allocation data for the latest month
        latest_data = df_savings_monthly.filter(pl.col("Month") == last_month)

        # Group by allocation type and category
        allocation_summary = latest_data.groupby(["AllocationType", "Category"]).agg(
            pl.col("TotalValue").sum().alias("Value")
        )

        # Convert to separate data for each allocation type
        allocation_types = allocation_summary["AllocationType"].unique().to_list()
        categories = allocation_summary["Category"].unique().to_list()

        # Build a figure with grouped bars
        fig = go.Figure()

        for alloc_type in allocation_types:
            filtered_data = allocation_summary.filter(
                pl.col("AllocationType") == alloc_type
            )

            # Get values for this allocation type
            values_by_category = {}
            for category in categories:
                match = filtered_data.filter(pl.col("Category") == category)
                values_by_category[category] = (
                    match["Value"][0] if len(match) > 0 else 0
                )

            # Get color for this allocation type
            color = (
                self.color_theme["savings"]["allocation"]
                if alloc_type == "Allocation"
                else self.color_theme["savings"]["spent"]
            )

            # Add bar trace
            fig.add_trace(
                go.Bar(
                    name=alloc_type,
                    x=list(values_by_category.keys()),
                    y=list(values_by_category.values()),
                    marker_color=color,
                )
            )

        # Update layout
        fig.update_layout(
            title=f"Allocation Status - {last_month}",
            yaxis=dict(title="Amount (€)"),
            plot_bgcolor=self.color_theme["background"],
            barmode="group",
            legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99),
        )

        return fig

    def setup_layout(self):
        # Calculate default date range (6 months before today)
        end_date = datetime.now().date()
        start_date = (end_date.replace(day=1) - timedelta(days=1)).replace(
            day=1
        ) - timedelta(days=5 * 30)

        self.app.layout = dbc.Container(
            [
                html.H1(
                    "Personal Finance Dashboard", className="text-center mt-4 mb-4"
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                dcc.DatePickerRange(
                                    id="date-range",
                                    min_date_allowed=self.df_spese["Date"].min(),
                                    max_date_allowed=self.df_spese["Date"].max(),
                                    start_date=start_date,
                                    end_date=end_date,
                                )
                            ],
                            width={"size": 6, "offset": 3},
                            className="text-center",
                        )
                    ],
                    className="mb-4",
                ),
                html.Div(id="summary-cards"),
                # Tabs for different views
                dbc.Tabs(
                    [
                        dbc.Tab(
                            [
                                dcc.Graph(id="main-dashboard"),
                                dcc.Graph(id="category-dashboard"),
                                dcc.Graph(id="stacked-expenses"),
                                dcc.Graph(id="stacked-income"),
                            ],
                            label="Income & Expenses",
                            tab_id="income-expenses-tab",
                        ),
                        dbc.Tab(
                            [
                                html.Div(id="savings-cards", className="mb-4 mt-4"),
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
                                html.H4("Savings Transactions", className="mt-4 mb-3"),
                                html.Div(id="savings-table"),
                            ],
                            label="Savings",
                            tab_id="savings-tab",
                        ),
                    ],
                    id="dashboard-tabs",
                    active_tab="income-expenses-tab",
                ),
            ],
            fluid=True,
        )

    def setup_callbacks(self):
        @self.app.callback(
            [
                Output("summary-cards", "children"),
                Output("main-dashboard", "figure"),
                Output("category-dashboard", "figure"),
                Output("stacked-expenses", "figure"),
                Output("stacked-income", "figure"),
                Output("savings-cards", "children"),
                Output("savings-overview", "figure"),
                Output("savings-breakdown", "figure"),
                Output("savings-allocation", "figure"),
                Output("savings-table", "children"),
            ],
            [Input("date-range", "start_date"), Input("date-range", "end_date")],
        )
        def update_dashboard(start_date, end_date):
            # Convert input strings to datetime.date
            if start_date:
                start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            if end_date:
                end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

            # Filter dataframes
            spese_filtered = self.df_spese.filter(
                (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
            )
            entrate_filtered = self.df_entrate.filter(
                (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
            )

            # Calculate monthly summaries
            monthly_summary = self._calculate_monthly_summary(start_date, end_date)
            expense_breakdown = self._calculate_monthly_breakdown(
                spese_filtered, "Expenses"
            )
            income_breakdown = self._calculate_monthly_breakdown(
                entrate_filtered, "Income"
            )

            # Filter savings data
            savings_filtered = self._filter_savings_data(start_date, end_date)
            savings_monthly_filtered = self._filter_savings_monthly_data(
                start_date, end_date
            )

            # Create dashboard elements
            cards = self.create_summary_cards(monthly_summary)
            fig_overview, fig_categories = self.create_figures(monthly_summary)
            fig_expenses = self.create_stacked_bar(
                expense_breakdown, "Expenses", "Monthly Expense Breakdown"
            )
            fig_income = self.create_stacked_bar(
                income_breakdown, "Income", "Monthly Income Breakdown"
            )

            # Create savings elements
            savings_cards = self.create_savings_summary_cards(savings_monthly_filtered)
            fig_savings = self.create_savings_figure(savings_monthly_filtered)
            fig_savings_breakdown = self.create_savings_breakdown_figure(
                savings_monthly_filtered
            )
            fig_savings_allocation = self.create_savings_allocation_figure(
                savings_monthly_filtered
            )
            savings_table = self.create_savings_table(savings_filtered)

            return (
                cards,
                fig_overview,
                fig_categories,
                fig_expenses,
                fig_income,
                savings_cards,
                fig_savings,
                fig_savings_breakdown,
                fig_savings_allocation,
                savings_table,
            )

    def run_server(self, debug=False, port=8050):
        print(f"Dashboard will run at http://127.0.0.1:{port}/")
        self.app.run(debug=debug, port=port)

    def create_summary_cards(self, monthly_summary):
        """Generate summary cards for total income, expenses, and balance."""
        total_income = monthly_summary["Income"].sum()
        total_expenses = monthly_summary["Expense"].sum()
        total_balance = total_income - total_expenses

        cards = dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Total Income (€)", className="card-title"),
                                html.H4(
                                    f"{total_income:.2f}",
                                    className="card-text",
                                    style={"color": self.color_theme["income"]},
                                ),
                            ]
                        ),
                        className="shadow-sm",
                    )
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Total Expenses (€)", className="card-title"),
                                html.H4(
                                    f"{total_expenses:.2f}",
                                    className="card-text",
                                    style={"color": self.color_theme["expense"]},
                                ),
                            ]
                        ),
                        className="shadow-sm",
                    )
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Balance (€)", className="card-title"),
                                html.H4(
                                    f"{total_balance:.2f}",
                                    className="card-text",
                                    style={"color": self.color_theme["balance"]},
                                ),
                            ]
                        ),
                        className="shadow-sm",
                    )
                ),
            ],
            className="mb-4",
        )

        return cards

    def create_savings_summary_cards(self, df_savings_monthly):
        """Generate summary cards for savings data."""
        if df_savings_monthly is None or len(df_savings_monthly) == 0:
            # Return an empty placeholder if no data
            return html.Div("No savings data available for the selected period.")

        # Get the latest month's data
        latest_month = df_savings_monthly["Month"].max()

        # Get total savings for the latest month
        total_savings = df_savings_monthly.filter(
            (pl.col("Month") == latest_month) & (pl.col("TotalSavings").is_not_null())
        )["TotalSavings"].max()

        if total_savings is None:
            total_savings = 0

        # Get allocation vs spent summary
        allocation_summary = (
            df_savings_monthly.filter(pl.col("Month") == latest_month)
            .groupby("AllocationType")
            .agg(pl.col("TotalValue").sum().alias("Total"))
        )

        # Extract values or default to 0
        allocated = allocation_summary.filter(pl.col("AllocationType") == "Allocation")[
            "Total"
        ].sum()
        if allocated is None:
            allocated = 0

        spent = allocation_summary.filter(pl.col("AllocationType") == "Spent")[
            "Total"
        ].sum()
        if spent is None:
            spent = 0

        cards = dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Total Savings (€)", className="card-title"),
                                html.H4(
                                    f"{total_savings:.2f}",
                                    className="card-text",
                                    style={
                                        "color": self.color_theme["savings"]["total"]
                                    },
                                ),
                                html.P(f"As of {latest_month}", className="text-muted"),
                            ]
                        ),
                        className="shadow-sm",
                    ),
                    width=4,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Allocated Funds (€)", className="card-title"),
                                html.H4(
                                    f"{allocated:.2f}",
                                    className="card-text",
                                    style={
                                        "color": self.color_theme["savings"][
                                            "allocation"
                                        ]
                                    },
                                ),
                                html.P(
                                    "Funds set aside but not spent",
                                    className="text-muted",
                                ),
                            ]
                        ),
                        className="shadow-sm",
                    ),
                    width=4,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Spent Funds (€)", className="card-title"),
                                html.H4(
                                    f"{spent:.2f}",
                                    className="card-text",
                                    style={
                                        "color": self.color_theme["savings"]["spent"]
                                    },
                                ),
                                html.P("Funds already spent", className="text-muted"),
                            ]
                        ),
                        className="shadow-sm",
                    ),
                    width=4,
                ),
            ],
            className="mb-4",
        )

        return cards

    def create_savings_table(self, df_savings):
        """Create a table to display savings transactions."""
        if df_savings is None or len(df_savings) == 0:
            return html.Div(
                "No savings transactions available for the selected period."
            )

        # Create a copy of the dataframe with sorted data
        df_table = df_savings.sort("Date", descending=True)

        # Add IsAllocation column if it doesn't exist
        if "IsAllocation" not in df_table.columns:
            # Handle allocation types based on transaction type
            df_table = df_table.with_columns(
                pl.when(pl.col("Type").is_in(["Allocation", "Accantonamento"]))
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias("IsAllocation")
            )

        # Format date and numeric columns for display
        df_table = df_table.with_columns(
            [
                pl.col("Date").dt.strftime("%d/%m/%Y").alias("Date"),
                pl.col("Value").map_elements(lambda x: f"€{x:.2f}").alias("Amount"),
            ]
        )

        # Select and rename columns for display
        df_display = df_table.select(
            [
                "Date",
                "Description",
                "Account",
                "Category",
                "Amount",
                pl.col("IsAllocation")
                .map_elements(lambda x: "Allocation" if x else "Spent")
                .alias("Type"),
            ]
        )

        # Convert directly to records for Dash without using pandas
        records = df_display.to_dicts()

        # Create the table
        table = dash_table.DataTable(
            data=records,
            columns=[{"name": col, "id": col} for col in df_display.columns],
            style_table={"overflowX": "auto"},
            style_cell={
                "textAlign": "left",
                "padding": "10px",
                "whiteSpace": "normal",
                "height": "auto",
            },
            style_header={
                "backgroundColor": "rgb(230, 230, 230)",
                "fontWeight": "bold",
            },
            style_data_conditional=[
                {
                    "if": {"filter_query": '{Type} = "Allocation"'},
                    "backgroundColor": "rgba(230, 126, 34, 0.2)",
                },
                {
                    "if": {"filter_query": '{Type} = "Spent"'},
                    "backgroundColor": "rgba(39, 174, 96, 0.2)",
                },
            ],
            page_size=10,
        )

        return table

    def create_figures(self, monthly_summary):
        """Create figures for overview and category distribution."""
        fig_overview = go.Figure()
        fig_overview.add_trace(
            go.Bar(
                x=monthly_summary["Month"],
                y=monthly_summary["Income"],
                name="Income",
                marker_color=self.color_theme["income"],
            )
        )
        fig_overview.add_trace(
            go.Bar(
                x=monthly_summary["Month"],
                y=monthly_summary["Expense"],
                name="Expense",
                marker_color=self.color_theme["expense"],
            )
        )
        fig_overview.add_trace(
            go.Scatter(
                x=monthly_summary["Month"],
                y=monthly_summary["Balance"],
                name="Balance",
                line=dict(color=self.color_theme["balance"], width=3),
            )
        )
        fig_overview.update_layout(
            title="Monthly Overview",
            barmode="group",
            yaxis=dict(title="Amount (€)"),
            plot_bgcolor=self.color_theme["background"],
            hovermode="x",
        )

        fig_categories = (
            go.Figure()
        )  # Placeholder for additional category visualizations
        return fig_overview, fig_categories
