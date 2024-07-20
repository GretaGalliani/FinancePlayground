import polars as pl
import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import dash_bootstrap_components as dbc
from datetime import datetime, timedelta


class ExpenseDashboard:
    def __init__(self, df):
        self.df = df
        self.df = self.df.sort("Date")
        self.app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        self.setup_layout()
        self.setup_callbacks()
        self.color_theme = px.colors.qualitative.Plotly

    def _aggregate_expenses(self, df, frequency="monthly"):
        if frequency == "monthly":
            return (
                df.with_columns(pl.col("Date").dt.strftime("%Y-%m").alias("Period"))
                .groupby(["Period", "Category"])
                .agg(pl.col("Value").sum().alias("Total"))
                .sort("Period")
            )
        elif frequency == "weekly":
            return (
                df.with_columns(
                    (pl.col("Date") - pl.duration(days=pl.col("Date").dt.weekday()))
                    .dt.strftime("%Y-%m-%d")
                    .alias("Period")
                )
                .groupby(["Period", "Category"])
                .agg(pl.col("Value").sum().alias("Total"))
                .sort("Period")
            )
        else:
            raise ValueError("Frequency must be 'monthly' or 'weekly'")

    def create_figures(self, df, frequency):
        fig = make_subplots(
            rows=3,
            cols=2,
            subplot_titles=(
                f"{frequency.capitalize()} Expenses",
                "Expenses by Category",
                "Top 5 Expenses",
                "Expense Trend",
                "Expense Growth Trend",
            ),
            specs=[
                [{"type": "bar"}, {"type": "pie"}],
                [{"type": "table"}, {"type": "scatter"}],
                [{"type": "scatter", "colspan": 2}, None],
            ],
            vertical_spacing=0.1,
            horizontal_spacing=0.05,
        )

        period_expenses = self._aggregate_expenses(df, frequency)

        categories = df["Category"].unique().to_list()
        category_colors = dict(zip(categories, self.color_theme))

        # Period Expenses Bar Chart
        for category in categories:
            category_data = period_expenses.filter(pl.col("Category") == category)
            fig.add_trace(
                go.Bar(
                    x=category_data["Period"].to_list(),
                    y=category_data["Total"].to_list(),
                    name=category,
                    marker_color=category_colors[category],
                    hovertemplate="%{x}<br>%{category}<br>%{y:,.2f}€<extra></extra>",
                ),
                row=1,
                col=1,
            )
        fig.update_layout(barmode="stack")

        # Expenses by Category Pie Chart
        category_expenses = df.groupby("Category").agg(
            pl.col("Value").sum().alias("Total")
        )
        fig.add_trace(
            go.Pie(
                labels=category_expenses["Category"].to_list(),
                values=category_expenses["Total"].to_list(),
                name="Expenses by Category",
                marker=dict(
                    colors=[
                        category_colors[cat] for cat in category_expenses["Category"]
                    ]
                ),
                showlegend=False,
                hovertemplate="%{label}<br>%{value:,.2f}€<extra></extra>",
            ),
            row=1,
            col=2,
        )

        # Top 5 Expenses Table
        top_expenses = df.sort("Value", descending=True).head(5)
        fig.add_trace(
            go.Table(
                header=dict(
                    values=[
                        "<b>Date</b>",
                        "<b>Description</b>",
                        "<b>Category</b>",
                        "<b>Value</b>",
                    ],
                    fill_color="lightgrey",
                    align="left",
                    font=dict(size=12),
                ),
                cells=dict(
                    values=[
                        top_expenses["Date"].dt.strftime("%Y-%m-%d").to_list(),
                        top_expenses["Description"].to_list(),
                        top_expenses["Category"].to_list(),
                        [f"{value:,.2f}€" for value in top_expenses["Value"].to_list()],
                    ],
                    align="left",
                    font=dict(size=11),
                    height=25,
                ),
            ),
            row=2,
            col=1,
        )

        # Expense Trend Line Chart
        trend_data = (
            period_expenses.groupby("Period").agg(pl.col("Total").sum()).sort("Period")
        )
        fig.add_trace(
            go.Scatter(
                x=trend_data["Period"].to_list(),
                y=trend_data["Total"].to_list(),
                mode="lines+markers",
                name="Expense Trend",
                showlegend=False,
                hovertemplate="%{x}<br>%{y:,.2f}€<extra></extra>",
            ),
            row=2,
            col=2,
        )

        # Expense Growth Trend
        growth_data = trend_data.with_columns(
            pl.col("Total").pct_change().alias("Growth")
        ).filter(pl.col("Growth").is_not_null())

        fig.add_trace(
            go.Scatter(
                x=growth_data["Period"].to_list(),
                y=growth_data["Growth"].to_list(),
                mode="lines+markers",
                name="Expense Growth Trend",
                showlegend=False,
                hovertemplate="%{x}<br>%{y:.2%}<extra></extra>",
            ),
            row=3,
            col=1,
        )

        fig.update_layout(
            height=1200,
            width=1200,
            title_text=f"Expense Dashboard ({frequency.capitalize()})",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            margin=dict(l=50, r=50, t=100, b=50),
            plot_bgcolor="white",
        )

        # Update axes to improve readability and add euro symbol
        fig.update_xaxes(tickangle=45, title_text="Period", gridcolor="lightgrey")
        fig.update_yaxes(
            title_text="Total Expenses", tickformat=",.2f€", gridcolor="lightgrey"
        )

        # Update y-axis for growth trend
        fig.update_yaxes(title_text="Growth Rate", tickformat=".2%", row=3, col=1)

        return fig

    def setup_layout(self):
        self.app.layout = dbc.Container(
            [
                html.H1("Expense Dashboard", className="mt-4 mb-4"),
                dbc.Tabs(
                    [
                        dbc.Tab(label="Monthly View", tab_id="monthly"),
                        dbc.Tab(label="Weekly View", tab_id="weekly"),
                    ],
                    id="tabs",
                    active_tab="monthly",
                ),
                html.Div(
                    [
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dcc.DatePickerRange(
                                            id="date-range",
                                            min_date_allowed=self.df["Date"].min(),
                                            max_date_allowed=self.df["Date"].max(),
                                            start_date=self.df["Date"].min(),
                                            end_date=self.df["Date"].max(),
                                        )
                                    ],
                                    width=6,
                                ),
                                dbc.Col(
                                    [
                                        dcc.Dropdown(
                                            id="category-filter",
                                            options=[
                                                {"label": cat, "value": cat}
                                                for cat in self.df["Category"].unique()
                                            ],
                                            multi=True,
                                            placeholder="Select categories to include",
                                        )
                                    ],
                                    width=6,
                                ),
                            ],
                            className="mt-4 mb-4",
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dcc.Dropdown(
                                            id="category-exclude",
                                            options=[
                                                {"label": cat, "value": cat}
                                                for cat in self.df["Category"].unique()
                                            ],
                                            multi=True,
                                            placeholder="Select categories to exclude",
                                        )
                                    ],
                                    width=6,
                                ),
                            ],
                            className="mt-4 mb-4",
                        ),
                    ]
                ),
                dcc.Graph(id="dashboard-graph"),
            ]
        )

    def setup_callbacks(self):
        @self.app.callback(
            Output("dashboard-graph", "figure"),
            [
                Input("tabs", "active_tab"),
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("category-filter", "value"),
                Input("category-exclude", "value"),
            ],
        )
        def update_graph(
            tab, start_date, end_date, categories_include, categories_exclude
        ):
            filtered_df = self.df.filter(
                (pl.col("Date") >= pl.lit(start_date).str.strptime(pl.Date, "%Y-%m-%d"))
                & (pl.col("Date") <= pl.lit(end_date).str.strptime(pl.Date, "%Y-%m-%d"))
            )
            if categories_include:
                filtered_df = filtered_df.filter(
                    pl.col("Category").is_in(categories_include)
                )
            if categories_exclude:
                filtered_df = filtered_df.filter(
                    ~pl.col("Category").is_in(categories_exclude)
                )
            return self.create_figures(
                filtered_df, "monthly" if tab == "monthly" else "weekly"
            )

    def run_server(self, debug=False, port=8050):
        print(f"Dashboard will run at http://127.0.0.1:{port}/")
        self.app.run(debug=debug, port=port)
