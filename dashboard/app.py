"""
dashboard/app.py
-----------------
Plotly Dash dashboard for Olympic Medals data.
Reads directly from Snowflake star schema.
Run: python dashboard/app.py  →  open http://localhost:8050
"""

import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

import dash
from dash import dcc, html, Input, Output, dash_table
import plotly.express as px
import plotly.graph_objects as go

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

MEDAL_COLORS = {
    "Gold":   "#FFD700",
    "Silver": "#C0C0C0",
    "Bronze": "#CD7F32",
}

# ─────────────────────────────────────────────
# Snowflake helpers
# ─────────────────────────────────────────────
def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


def query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql(sql, conn)
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()


# ─────────────────────────────────────────────
# Load all data once at startup
# ─────────────────────────────────────────────
def load_data():
    print("Loading data from Snowflake...")

    medals = query("""
        SELECT
            f.medal_type,
            f.country_code,
            dc.country_name,
            ds.sport_name,
            ds.category        AS sport_category,
            de.event_name,
            de.gender,
            dg.year,
            dg.season,
            dg.host_city,
            dg.host_country,
            da.full_name       AS athlete_name
        FROM fact_medals f
        JOIN dim_country dc ON f.country_code = dc.country_code
        JOIN dim_sport   ds ON f.sport_id     = ds.sport_id
        JOIN dim_event   de ON f.event_id     = de.event_id
        JOIN dim_games   dg ON f.games_id     = dg.games_id
        JOIN dim_athlete da ON f.athlete_id   = da.athlete_id
        ORDER BY dg.year
    """)

    print(f"Loaded {len(medals)} medal records.")
    return medals


try:
    DF = load_data()
    DATA_LOADED = True
except Exception as e:
    print(f"WARNING: Could not load from Snowflake: {e}")
    print("Dashboard will show empty state.")
    DF = pd.DataFrame(columns=[
        "medal_type","country_code","country_name","sport_name",
        "sport_category","event_name","gender","year","season",
        "host_city","host_country","athlete_name"
    ])
    DATA_LOADED = False

# Derived lists for dropdowns
ALL_COUNTRIES = sorted(DF["country_name"].dropna().unique().tolist()) if DATA_LOADED else []
ALL_SPORTS    = sorted(DF["sport_name"].dropna().unique().tolist())   if DATA_LOADED else []
ALL_YEARS     = sorted(DF["year"].dropna().unique().tolist())         if DATA_LOADED else []
ALL_SEASONS   = sorted(DF["season"].dropna().unique().tolist())       if DATA_LOADED else []

# ─────────────────────────────────────────────
# App layout
# ─────────────────────────────────────────────
app = dash.Dash(
    __name__,
    title="🏅 Olympic Medals Dashboard",
    suppress_callback_exceptions=True,
)

# ── Styles ──────────────────────────────────
CARD_STYLE = {
    "background": "#1e1e2e",
    "borderRadius": "12px",
    "padding": "20px",
    "marginBottom": "20px",
    "boxShadow": "0 4px 20px rgba(0,0,0,0.4)",
}

KPI_STYLE = {
    **CARD_STYLE,
    "textAlign": "center",
    "padding": "24px 16px",
}

LABEL_STYLE = {
    "color": "#a0aec0",
    "fontSize": "12px",
    "marginBottom": "6px",
    "fontWeight": "600",
    "letterSpacing": "0.5px",
}

DROPDOWN_STYLE = {
    "background": "#2d2d3f",
    "color": "#fff",
    "border": "1px solid #4a4a6a",
    "borderRadius": "8px",
}

# ── Header ───────────────────────────────────
header = html.Div([
    html.H1("🏅 Olympic Medals Dashboard", style={
        "color": "#fff",
        "fontSize": "28px",
        "fontWeight": "700",
        "margin": "0",
    }),
    html.P("Explore Olympic medal data across countries, sports, and years",
           style={"color": "#a0aec0", "margin": "4px 0 0 0", "fontSize": "14px"}),
], style={
    "background": "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)",
    "padding": "24px 32px",
    "borderBottom": "1px solid #2d2d4e",
    "marginBottom": "24px",
})

# ── Filters ──────────────────────────────────
filters = html.Div([
    html.Div([
        html.Label("Season", style=LABEL_STYLE),
        dcc.Dropdown(
            id="filter-season",
            options=[{"label": s, "value": s} for s in ALL_SEASONS],
            multi=True,
            placeholder="All Seasons",
            style=DROPDOWN_STYLE,
        ),
    ], style={"flex": "1", "marginRight": "16px"}),

    html.Div([
        html.Label("Year Range", style=LABEL_STYLE),
        dcc.RangeSlider(
            id="filter-year",
            min=int(min(ALL_YEARS)) if ALL_YEARS else 1948,
            max=int(max(ALL_YEARS)) if ALL_YEARS else 2024,
            value=[int(min(ALL_YEARS)) if ALL_YEARS else 1948,
                   int(max(ALL_YEARS)) if ALL_YEARS else 2024],
            marks={y: str(y) for y in ALL_YEARS[::4]} if ALL_YEARS else {},
            tooltip={"placement": "bottom", "always_visible": False},
        ),
    ], style={"flex": "3", "marginRight": "16px"}),

    html.Div([
        html.Label("Medal Type", style=LABEL_STYLE),
        dcc.Dropdown(
            id="filter-medal",
            options=[{"label": m, "value": m} for m in ["Gold", "Silver", "Bronze"]],
            multi=True,
            placeholder="All Medals",
            style=DROPDOWN_STYLE,
        ),
    ], style={"flex": "1"}),

], style={
    "display": "flex",
    "alignItems": "flex-end",
    "background": "#1e1e2e",
    "padding": "20px 24px",
    "borderRadius": "12px",
    "marginBottom": "20px",
    "boxShadow": "0 4px 20px rgba(0,0,0,0.3)",
})

# ── KPI Cards ────────────────────────────────
kpi_row = html.Div([
    html.Div([
        html.Div("🏅", style={"fontSize": "32px"}),
        html.Div(id="kpi-total",   style={"fontSize": "36px", "fontWeight": "700", "color": "#fff"}),
        html.Div("Total Medals",   style={"color": "#a0aec0", "fontSize": "13px", "marginTop": "4px"}),
    ], style=KPI_STYLE),

    html.Div([
        html.Div("🥇", style={"fontSize": "32px"}),
        html.Div(id="kpi-gold",    style={"fontSize": "36px", "fontWeight": "700", "color": MEDAL_COLORS["Gold"]}),
        html.Div("Gold Medals",    style={"color": "#a0aec0", "fontSize": "13px", "marginTop": "4px"}),
    ], style=KPI_STYLE),

    html.Div([
        html.Div("🌍", style={"fontSize": "32px"}),
        html.Div(id="kpi-countries", style={"fontSize": "36px", "fontWeight": "700", "color": "#63b3ed"}),
        html.Div("Countries",        style={"color": "#a0aec0", "fontSize": "13px", "marginTop": "4px"}),
    ], style=KPI_STYLE),

    html.Div([
        html.Div("🏃", style={"fontSize": "32px"}),
        html.Div(id="kpi-athletes",  style={"fontSize": "36px", "fontWeight": "700", "color": "#68d391"}),
        html.Div("Athletes",          style={"color": "#a0aec0", "fontSize": "13px", "marginTop": "4px"}),
    ], style=KPI_STYLE),

    html.Div([
        html.Div("⚽", style={"fontSize": "32px"}),
        html.Div(id="kpi-sports",    style={"fontSize": "36px", "fontWeight": "700", "color": "#f6ad55"}),
        html.Div("Sports",            style={"color": "#a0aec0", "fontSize": "13px", "marginTop": "4px"}),
    ], style=KPI_STYLE),

], style={
    "display": "grid",
    "gridTemplateColumns": "repeat(5, 1fr)",
    "gap": "16px",
    "marginBottom": "20px",
})

# ── Charts Row 1 ─────────────────────────────
row1 = html.Div([
    html.Div([
        html.H3("Top 15 Countries by Total Medals", style={"color": "#fff", "fontSize": "16px", "marginTop": 0}),
        dcc.Graph(id="chart-top-countries", config={"displayModeBar": False}),
    ], style={**CARD_STYLE, "flex": "1.4", "marginRight": "16px"}),

    html.Div([
        html.H3("Medal Distribution by Type", style={"color": "#fff", "fontSize": "16px", "marginTop": 0}),
        dcc.Graph(id="chart-medal-pie", config={"displayModeBar": False}),
    ], style={**CARD_STYLE, "flex": "0.6"}),

], style={"display": "flex", "marginBottom": "0"})

# ── Charts Row 2 ─────────────────────────────
row2 = html.Div([
    html.Div([
        html.H3("Medals Over Time", style={"color": "#fff", "fontSize": "16px", "marginTop": 0}),
        dcc.Graph(id="chart-timeline", config={"displayModeBar": False}),
    ], style={**CARD_STYLE, "flex": "1", "marginRight": "16px"}),

    html.Div([
        html.H3("Top 10 Sports", style={"color": "#fff", "fontSize": "16px", "marginTop": 0}),
        dcc.Graph(id="chart-sports", config={"displayModeBar": False}),
    ], style={**CARD_STYLE, "flex": "0.8"}),

], style={"display": "flex", "marginBottom": "0"})

# ── Charts Row 3 ─────────────────────────────
row3 = html.Div([
    html.Div([
        html.H3("Medals by Sport Category", style={"color": "#fff", "fontSize": "16px", "marginTop": 0}),
        dcc.Graph(id="chart-category", config={"displayModeBar": False}),
    ], style={**CARD_STYLE, "flex": "0.8", "marginRight": "16px"}),

    html.Div([
        html.H3("Gender Distribution", style={"color": "#fff", "fontSize": "16px", "marginTop": 0}),
        dcc.Graph(id="chart-gender", config={"displayModeBar": False}),
    ], style={**CARD_STYLE, "flex": "0.6", "marginRight": "16px"}),

    html.Div([
        html.H3("Country Deep Dive", style={"color": "#fff", "fontSize": "16px", "marginTop": 0}),
        dcc.Dropdown(
            id="filter-country",
            options=[{"label": c, "value": c} for c in ALL_COUNTRIES],
            value=ALL_COUNTRIES[0] if ALL_COUNTRIES else None,
            clearable=False,
            style=DROPDOWN_STYLE,
        ),
        dcc.Graph(id="chart-country-drill", config={"displayModeBar": False}),
    ], style={**CARD_STYLE, "flex": "0.8"}),

], style={"display": "flex", "marginBottom": "0"})

# ── Table ─────────────────────────────────────
table_section = html.Div([
    html.H3("Medal Records", style={"color": "#fff", "fontSize": "16px", "marginTop": 0}),
    dash_table.DataTable(
        id="table-medals",
        columns=[
            {"name": "Athlete",   "id": "athlete_name"},
            {"name": "Country",   "id": "country_name"},
            {"name": "Sport",     "id": "sport_name"},
            {"name": "Event",     "id": "event_name"},
            {"name": "Year",      "id": "year"},
            {"name": "Season",    "id": "season"},
            {"name": "Medal",     "id": "medal_type"},
        ],
        page_size=15,
        sort_action="native",
        filter_action="native",
        style_table={"overflowX": "auto"},
        style_header={
            "backgroundColor": "#2d2d4e",
            "color": "#fff",
            "fontWeight": "700",
            "border": "1px solid #4a4a6a",
        },
        style_cell={
            "backgroundColor": "#1a1a2e",
            "color": "#e2e8f0",
            "border": "1px solid #2d2d4e",
            "padding": "10px 14px",
            "fontSize": "13px",
        },
        style_data_conditional=[
            {"if": {"filter_query": '{medal_type} = "Gold"'},   "color": MEDAL_COLORS["Gold"]},
            {"if": {"filter_query": '{medal_type} = "Silver"'}, "color": MEDAL_COLORS["Silver"]},
            {"if": {"filter_query": '{medal_type} = "Bronze"'}, "color": MEDAL_COLORS["Bronze"]},
            {"if": {"row_index": "odd"}, "backgroundColor": "#16213e"},
        ],
    ),
], style=CARD_STYLE)

# ── Main layout ──────────────────────────────
app.layout = html.Div([
    header,
    html.Div([
        filters,
        kpi_row,
        row1,
        html.Div(style={"height": "20px"}),
        row2,
        html.Div(style={"height": "20px"}),
        row3,
        html.Div(style={"height": "20px"}),
        table_section,
    ], style={"padding": "0 32px 32px 32px"}),
], style={
    "fontFamily": "'Inter', 'Segoe UI', sans-serif",
    "background": "#0f0f1a",
    "minHeight": "100vh",
})


# ─────────────────────────────────────────────
# Helper: apply filters to dataframe
# ─────────────────────────────────────────────
def apply_filters(seasons, year_range, medals):
    dff = DF.copy()
    if seasons:
        dff = dff[dff["season"].isin(seasons)]
    if year_range and len(year_range) == 2:
        dff = dff[(dff["year"] >= year_range[0]) & (dff["year"] <= year_range[1])]
    if medals:
        dff = dff[dff["medal_type"].isin(medals)]
    return dff


def dark_layout(fig, height=320):
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e2e8f0", size=12),
        height=height,
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#e2e8f0")),
    )
    fig.update_xaxes(gridcolor="#2d2d4e", zerolinecolor="#2d2d4e")
    fig.update_yaxes(gridcolor="#2d2d4e", zerolinecolor="#2d2d4e")
    return fig


# ─────────────────────────────────────────────
# Callbacks
# ─────────────────────────────────────────────

# KPIs
@app.callback(
    Output("kpi-total",     "children"),
    Output("kpi-gold",      "children"),
    Output("kpi-countries", "children"),
    Output("kpi-athletes",  "children"),
    Output("kpi-sports",    "children"),
    Input("filter-season", "value"),
    Input("filter-year",   "value"),
    Input("filter-medal",  "value"),
)
def update_kpis(seasons, year_range, medals):
    dff = apply_filters(seasons, year_range, medals)
    total     = f"{len(dff):,}"
    gold      = f"{len(dff[dff['medal_type'] == 'Gold']):,}"
    countries = f"{dff['country_code'].nunique():,}"
    athletes  = f"{dff['athlete_name'].nunique():,}"
    sports    = f"{dff['sport_name'].nunique():,}"
    return total, gold, countries, athletes, sports


# Top 15 countries bar chart
@app.callback(
    Output("chart-top-countries", "figure"),
    Input("filter-season", "value"),
    Input("filter-year",   "value"),
    Input("filter-medal",  "value"),
)
def update_top_countries(seasons, year_range, medals):
    dff = apply_filters(seasons, year_range, medals)
    if dff.empty:
        return go.Figure()

    agg = (
        dff.groupby(["country_name", "medal_type"])
        .size()
        .reset_index(name="count")
    )
    top15 = (
        agg.groupby("country_name")["count"]
        .sum()
        .nlargest(15)
        .index.tolist()
    )
    agg = agg[agg["country_name"].isin(top15)]
    # Sort by total
    order = (
        agg.groupby("country_name")["count"]
        .sum()
        .sort_values(ascending=True)
        .index.tolist()
    )

    fig = px.bar(
        agg,
        x="count",
        y="country_name",
        color="medal_type",
        color_discrete_map=MEDAL_COLORS,
        orientation="h",
        category_orders={"country_name": order, "medal_type": ["Bronze", "Silver", "Gold"]},
    )
    return dark_layout(fig, height=380)


# Medal type pie
@app.callback(
    Output("chart-medal-pie", "figure"),
    Input("filter-season", "value"),
    Input("filter-year",   "value"),
    Input("filter-medal",  "value"),
)
def update_pie(seasons, year_range, medals):
    dff = apply_filters(seasons, year_range, medals)
    if dff.empty:
        return go.Figure()

    counts = dff["medal_type"].value_counts().reset_index()
    counts.columns = ["medal_type", "count"]
    fig = px.pie(
        counts,
        names="medal_type",
        values="count",
        color="medal_type",
        color_discrete_map=MEDAL_COLORS,
        hole=0.45,
    )
    fig.update_traces(textfont_color="#fff")
    return dark_layout(fig, height=380)


# Timeline
@app.callback(
    Output("chart-timeline", "figure"),
    Input("filter-season", "value"),
    Input("filter-year",   "value"),
    Input("filter-medal",  "value"),
)
def update_timeline(seasons, year_range, medals):
    dff = apply_filters(seasons, year_range, medals)
    if dff.empty:
        return go.Figure()

    agg = (
        dff.groupby(["year", "medal_type"])
        .size()
        .reset_index(name="count")
    )
    fig = px.line(
        agg,
        x="year",
        y="count",
        color="medal_type",
        color_discrete_map=MEDAL_COLORS,
        markers=True,
    )
    return dark_layout(fig, height=300)


# Top 10 sports
@app.callback(
    Output("chart-sports", "figure"),
    Input("filter-season", "value"),
    Input("filter-year",   "value"),
    Input("filter-medal",  "value"),
)
def update_sports(seasons, year_range, medals):
    dff = apply_filters(seasons, year_range, medals)
    if dff.empty:
        return go.Figure()

    top10 = dff["sport_name"].value_counts().nlargest(10).reset_index()
    top10.columns = ["sport_name", "count"]
    top10 = top10.sort_values("count", ascending=True)

    fig = px.bar(
        top10,
        x="count",
        y="sport_name",
        orientation="h",
        color="count",
        color_continuous_scale="Viridis",
    )
    fig.update_layout(coloraxis_showscale=False)
    return dark_layout(fig, height=300)


# Sport category
@app.callback(
    Output("chart-category", "figure"),
    Input("filter-season", "value"),
    Input("filter-year",   "value"),
    Input("filter-medal",  "value"),
)
def update_category(seasons, year_range, medals):
    dff = apply_filters(seasons, year_range, medals)
    if dff.empty:
        return go.Figure()

    agg = dff["sport_category"].value_counts().reset_index()
    agg.columns = ["category", "count"]
    fig = px.bar(
        agg.sort_values("count", ascending=True),
        x="count",
        y="category",
        orientation="h",
        color="count",
        color_continuous_scale="Blues",
    )
    fig.update_layout(coloraxis_showscale=False)
    return dark_layout(fig, height=300)


# Gender donut
@app.callback(
    Output("chart-gender", "figure"),
    Input("filter-season", "value"),
    Input("filter-year",   "value"),
    Input("filter-medal",  "value"),
)
def update_gender(seasons, year_range, medals):
    dff = apply_filters(seasons, year_range, medals)
    if dff.empty:
        return go.Figure()

    counts = dff["gender"].value_counts().reset_index()
    counts.columns = ["gender", "count"]
    fig = px.pie(
        counts,
        names="gender",
        values="count",
        hole=0.5,
        color_discrete_sequence=["#63b3ed", "#f687b3", "#68d391"],
    )
    fig.update_traces(textfont_color="#fff")
    return dark_layout(fig, height=300)


# Country drill-down
@app.callback(
    Output("chart-country-drill", "figure"),
    Input("filter-country", "value"),
    Input("filter-season",  "value"),
    Input("filter-year",    "value"),
    Input("filter-medal",   "value"),
)
def update_country_drill(country, seasons, year_range, medals):
    dff = apply_filters(seasons, year_range, medals)
    if dff.empty or not country:
        return go.Figure()

    dff = dff[dff["country_name"] == country]
    agg = dff.groupby(["sport_name", "medal_type"]).size().reset_index(name="count")
    top_sports = agg.groupby("sport_name")["count"].sum().nlargest(8).index
    agg = agg[agg["sport_name"].isin(top_sports)]

    fig = px.bar(
        agg,
        x="sport_name",
        y="count",
        color="medal_type",
        color_discrete_map=MEDAL_COLORS,
        barmode="stack",
    )
    fig.update_xaxes(tickangle=-30)
    return dark_layout(fig, height=260)


# Data table
@app.callback(
    Output("table-medals", "data"),
    Input("filter-season", "value"),
    Input("filter-year",   "value"),
    Input("filter-medal",  "value"),
)
def update_table(seasons, year_range, medals):
    dff = apply_filters(seasons, year_range, medals)
    cols = ["athlete_name", "country_name", "sport_name", "event_name", "year", "season", "medal_type"]
    return dff[cols].head(500).to_dict("records")


# ─────────────────────────────────────────────
# Run
# ─────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)