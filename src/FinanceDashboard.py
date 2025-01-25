import polars as pl
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
import dash
from datetime import datetime, timedelta


class FinanceDashboard:
    def __init__(self, df_spese, df_entrate):
        self.df_spese = df_spese.sort("Date")
        self.df_entrate = df_entrate.sort("Date")

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
                dcc.Graph(id="main-dashboard"),
                dcc.Graph(id="category-dashboard"),
                dcc.Graph(id="stacked-expenses"),
                dcc.Graph(id="stacked-income"),
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

            # Create dashboard elements
            cards = self.create_summary_cards(monthly_summary)
            fig_overview, fig_categories = self.create_figures(monthly_summary)
            fig_expenses = self.create_stacked_bar(
                expense_breakdown, "Expenses", "Monthly Expense Breakdown"
            )
            fig_income = self.create_stacked_bar(
                income_breakdown, "Income", "Monthly Income Breakdown"
            )

            return cards, fig_overview, fig_categories, fig_expenses, fig_income

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
