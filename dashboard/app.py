import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import dash
from dash import dcc, html, Input, Output, dash_table
import plotly.express as px
import plotly.graph_objects as go

load_dotenv(dotenv_path=r"D:\DEPI\medalists_project\.env", override=True)

MEDAL_COLORS = {
    "Gold":   "#FFD700",
    "Silver": "#C0C0C0",
    "Bronze": "#CD7F32",
}

def get_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE", "OLYMPIC_MEDALS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )

def query(sql):
    conn = get_conn()
    try:
        df = pd.read_sql(sql, conn)
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()

print("Loading data from Snowflake...")
try:
    DF = query("""
        SELECT
            f.medal_type,
            f.country_code,
            dc.country_name,
            ds.sport_name,
            de.event_name,
            de.gender,
            dg.year,
            dg.season,
            da.full_name AS athlete_name
        FROM fact_medals f
        JOIN dim_country dc ON f.country_code = dc.country_code
        JOIN dim_sport   ds ON f.sport_id     = ds.sport_id
        JOIN dim_event   de ON f.event_id     = de.event_id
        JOIN dim_games   dg ON f.games_id     = dg.games_id
        JOIN dim_athlete da ON f.athlete_id   = da.athlete_id
    """)
    print(f"Loaded {len(DF)} records")
    DATA_OK = True
except Exception as e:
    print(f"ERROR loading data: {e}")
    DF = pd.DataFrame(columns=["medal_type","country_code","country_name",
                                "sport_name","event_name","gender",
                                "year","season","athlete_name"])
    DATA_OK = False

ALL_COUNTRIES = sorted(DF["country_name"].dropna().unique().tolist())
ALL_YEARS     = sorted(DF["year"].dropna().unique().tolist())
ALL_SEASONS   = sorted(DF["season"].dropna().unique().tolist())

CARD = {
    "background": "#1e1e2e",
    "borderRadius": "12px",
    "padding": "20px",
    "marginBottom": "20px",
    "boxShadow": "0 4px 20px rgba(0,0,0,0.4)",
}
KPI = {**CARD, "textAlign": "center", "padding": "24px 16px"}
DD  = {"background": "#2d2d3f", "color": "#fff", "border": "1px solid #4a4a6a", "borderRadius": "8px"}

app = dash.Dash(__name__, title="Olympic Medals Dashboard")

app.layout = html.Div([
    html.Div([
        html.H1("🏅 Olympic Medals Dashboard",
                style={"color": "#fff", "fontSize": "26px", "margin": 0}),
        html.P("Scraping -> ETL -> dbt -> Snowflake -> Dashboard",
               style={"color": "#a0aec0", "margin": "4px 0 0 0"}),
    ], style={"background": "#16213e", "padding": "20px 32px",
              "borderBottom": "1px solid #2d2d4e", "marginBottom": "24px"}),

    html.Div([
        html.Div([
            html.Div([
                html.Label("Season", style={"color": "#a0aec0", "fontSize": "12px"}),
                dcc.Dropdown(id="f-season",
                             options=[{"label": s, "value": s} for s in ALL_SEASONS],
                             multi=True, placeholder="All Seasons", style=DD),
            ], style={"flex": "1", "marginRight": "16px"}),
            html.Div([
                html.Label("Year Range", style={"color": "#a0aec0", "fontSize": "12px"}),
                dcc.RangeSlider(
                    id="f-year",
                    min=int(min(ALL_YEARS)) if ALL_YEARS else 1948,
                    max=int(max(ALL_YEARS)) if ALL_YEARS else 2024,
                    value=[int(min(ALL_YEARS)) if ALL_YEARS else 1948,
                           int(max(ALL_YEARS)) if ALL_YEARS else 2024],
                    marks={y: str(y) for y in ALL_YEARS[::4]} if ALL_YEARS else {},
                    tooltip={"placement": "bottom"},
                ),
            ], style={"flex": "3", "marginRight": "16px"}),
            html.Div([
                html.Label("Medal", style={"color": "#a0aec0", "fontSize": "12px"}),
                dcc.Dropdown(id="f-medal",
                             options=[{"label": m, "value": m} for m in ["Gold","Silver","Bronze"]],
                             multi=True, placeholder="All Medals", style=DD),
            ], style={"flex": "1"}),
        ], style={**CARD, "display": "flex", "alignItems": "flex-end"}),

        html.Div([
            html.Div([html.Div("🏅", style={"fontSize": "28px"}),
                      html.Div(id="kpi-total", style={"fontSize": "32px", "fontWeight": "700", "color": "#fff"}),
                      html.Div("Total Medals", style={"color": "#a0aec0", "fontSize": "12px"})], style=KPI),
            html.Div([html.Div("🥇", style={"fontSize": "28px"}),
                      html.Div(id="kpi-gold", style={"fontSize": "32px", "fontWeight": "700", "color": "#FFD700"}),
                      html.Div("Gold Medals", style={"color": "#a0aec0", "fontSize": "12px"})], style=KPI),
            html.Div([html.Div("🌍", style={"fontSize": "28px"}),
                      html.Div(id="kpi-countries", style={"fontSize": "32px", "fontWeight": "700", "color": "#63b3ed"}),
                      html.Div("Countries", style={"color": "#a0aec0", "fontSize": "12px"})], style=KPI),
            html.Div([html.Div("🏃", style={"fontSize": "28px"}),
                      html.Div(id="kpi-athletes", style={"fontSize": "32px", "fontWeight": "700", "color": "#68d391"}),
                      html.Div("Athletes", style={"color": "#a0aec0", "fontSize": "12px"})], style=KPI),
            html.Div([html.Div("⚽", style={"fontSize": "28px"}),
                      html.Div(id="kpi-sports", style={"fontSize": "32px", "fontWeight": "700", "color": "#f6ad55"}),
                      html.Div("Sports", style={"color": "#a0aec0", "fontSize": "12px"})], style=KPI),
        ], style={"display": "grid", "gridTemplateColumns": "repeat(5,1fr)", "gap": "16px", "marginBottom": "20px"}),

        html.Div([
            html.Div([
                html.H3("Top 15 Countries", style={"color": "#fff", "fontSize": "15px", "marginTop": 0}),
                dcc.Graph(id="chart-countries", config={"displayModeBar": False}),
            ], style={**CARD, "flex": "1.5", "marginRight": "16px"}),
            html.Div([
                html.H3("Medal Types", style={"color": "#fff", "fontSize": "15px", "marginTop": 0}),
                dcc.Graph(id="chart-pie", config={"displayModeBar": False}),
            ], style={**CARD, "flex": "0.8"}),
        ], style={"display": "flex", "marginBottom": "0"}),

        html.Div(style={"height": "20px"}),

        html.Div([
            html.Div([
                html.H3("Medals Over Time", style={"color": "#fff", "fontSize": "15px", "marginTop": 0}),
                dcc.Graph(id="chart-timeline", config={"displayModeBar": False}),
            ], style={**CARD, "flex": "1", "marginRight": "16px"}),
            html.Div([
                html.H3("Top Sports", style={"color": "#fff", "fontSize": "15px", "marginTop": 0}),
                dcc.Graph(id="chart-sports", config={"displayModeBar": False}),
            ], style={**CARD, "flex": "0.8"}),
        ], style={"display": "flex", "marginBottom": "0"}),

        html.Div(style={"height": "20px"}),

        html.Div([
            html.Div([
                html.H3("Gender Split", style={"color": "#fff", "fontSize": "15px", "marginTop": 0}),
                dcc.Graph(id="chart-gender", config={"displayModeBar": False}),
            ], style={**CARD, "flex": "0.8", "marginRight": "16px"}),
            html.Div([
                html.H3("Country Deep Dive", style={"color": "#fff", "fontSize": "15px", "marginTop": 0}),
                dcc.Dropdown(id="f-country",
                             options=[{"label": c, "value": c} for c in ALL_COUNTRIES],
                             value=ALL_COUNTRIES[0] if ALL_COUNTRIES else None,
                             clearable=False, style=DD),
                dcc.Graph(id="chart-drill", config={"displayModeBar": False}),
            ], style={**CARD, "flex": "1.2"}),
        ], style={"display": "flex", "marginBottom": "0"}),

        html.Div(style={"height": "20px"}),

        html.Div([
            html.H3("Medal Records", style={"color": "#fff", "fontSize": "15px", "marginTop": 0}),
            dash_table.DataTable(
                id="tbl",
                columns=[
                    {"name": "Athlete",  "id": "athlete_name"},
                    {"name": "Country",  "id": "country_name"},
                    {"name": "Sport",    "id": "sport_name"},
                    {"name": "Event",    "id": "event_name"},
                    {"name": "Year",     "id": "year"},
                    {"name": "Medal",    "id": "medal_type"},
                ],
                page_size=15,
                sort_action="native",
                filter_action="native",
                style_table={"overflowX": "auto"},
                style_header={"backgroundColor": "#2d2d4e", "color": "#fff", "fontWeight": "700"},
                style_cell={"backgroundColor": "#1a1a2e", "color": "#e2e8f0",
                            "border": "1px solid #2d2d4e", "padding": "10px"},
                style_data_conditional=[
                    {"if": {"filter_query": '{medal_type} = "Gold"'},   "color": "#FFD700"},
                    {"if": {"filter_query": '{medal_type} = "Silver"'}, "color": "#C0C0C0"},
                    {"if": {"filter_query": '{medal_type} = "Bronze"'}, "color": "#CD7F32"},
                    {"if": {"row_index": "odd"}, "backgroundColor": "#16213e"},
                ],
            ),
        ], style=CARD),

    ], style={"padding": "0 32px 32px 32px"}),

], style={"fontFamily": "'Segoe UI', sans-serif", "background": "#0f0f1a", "minHeight": "100vh"})


def filt(seasons, year_range, medals):
    d = DF.copy()
    if seasons:
        d = d[d["season"].isin(seasons)]
    if year_range and len(year_range) == 2:
        d = d[(d["year"] >= year_range[0]) & (d["year"] <= year_range[1])]
    if medals:
        d = d[d["medal_type"].isin(medals)]
    return d

def dark(fig, h=320):
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e2e8f0"), height=h,
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
    )
    fig.update_xaxes(gridcolor="#2d2d4e")
    fig.update_yaxes(gridcolor="#2d2d4e")
    return fig


@app.callback(
    Output("kpi-total","children"), Output("kpi-gold","children"),
    Output("kpi-countries","children"), Output("kpi-athletes","children"),
    Output("kpi-sports","children"),
    Input("f-season","value"), Input("f-year","value"), Input("f-medal","value"),
)
def kpis(seasons, yr, medals):
    d = filt(seasons, yr, medals)
    return (f"{len(d):,}", f"{len(d[d['medal_type']=='Gold']):,}",
            f"{d['country_code'].nunique():,}", f"{d['athlete_name'].nunique():,}",
            f"{d['sport_name'].nunique():,}")


@app.callback(Output("chart-countries","figure"),
              Input("f-season","value"), Input("f-year","value"), Input("f-medal","value"))
def top_countries(seasons, yr, medals):
    d = filt(seasons, yr, medals)
    if d.empty: return go.Figure()
    agg = d.groupby(["country_name","medal_type"]).size().reset_index(name="count")
    top = agg.groupby("country_name")["count"].sum().nlargest(15).index
    agg = agg[agg["country_name"].isin(top)]
    order = agg.groupby("country_name")["count"].sum().sort_values().index.tolist()
    fig = px.bar(agg, x="count", y="country_name", color="medal_type",
                 color_discrete_map=MEDAL_COLORS, orientation="h",
                 category_orders={"country_name": order, "medal_type": ["Bronze","Silver","Gold"]})
    return dark(fig, 380)


@app.callback(Output("chart-pie","figure"),
              Input("f-season","value"), Input("f-year","value"), Input("f-medal","value"))
def pie(seasons, yr, medals):
    d = filt(seasons, yr, medals)
    if d.empty: return go.Figure()
    c = d["medal_type"].value_counts().reset_index()
    c.columns = ["medal_type","count"]
    fig = px.pie(c, names="medal_type", values="count",
                 color="medal_type", color_discrete_map=MEDAL_COLORS, hole=0.45)
    return dark(fig, 380)


@app.callback(Output("chart-timeline","figure"),
              Input("f-season","value"), Input("f-year","value"), Input("f-medal","value"))
def timeline(seasons, yr, medals):
    d = filt(seasons, yr, medals)
    if d.empty: return go.Figure()
    agg = d.groupby(["year","medal_type"]).size().reset_index(name="count")
    fig = px.line(agg, x="year", y="count", color="medal_type",
                  color_discrete_map=MEDAL_COLORS, markers=True)
    return dark(fig, 300)


@app.callback(Output("chart-sports","figure"),
              Input("f-season","value"), Input("f-year","value"), Input("f-medal","value"))
def sports_chart(seasons, yr, medals):
    d = filt(seasons, yr, medals)
    if d.empty: return go.Figure()
    top = d["sport_name"].value_counts().nlargest(10).reset_index()
    top.columns = ["sport_name","count"]
    fig = px.bar(top.sort_values("count"), x="count", y="sport_name",
                 orientation="h", color="count", color_continuous_scale="Viridis")
    fig.update_layout(coloraxis_showscale=False)
    return dark(fig, 300)


@app.callback(Output("chart-gender","figure"),
              Input("f-season","value"), Input("f-year","value"), Input("f-medal","value"))
def gender_chart(seasons, yr, medals):
    d = filt(seasons, yr, medals)
    if d.empty: return go.Figure()
    c = d["gender"].value_counts().reset_index()
    c.columns = ["gender","count"]
    fig = px.pie(c, names="gender", values="count", hole=0.5,
                 color_discrete_sequence=["#63b3ed","#f687b3","#68d391"])
    return dark(fig, 300)


@app.callback(Output("chart-drill","figure"),
              Input("f-country","value"),
              Input("f-season","value"), Input("f-year","value"), Input("f-medal","value"))
def drill(country, seasons, yr, medals):
    d = filt(seasons, yr, medals)
    if d.empty or not country: return go.Figure()
    d = d[d["country_name"] == country]
    agg = d.groupby(["sport_name","medal_type"]).size().reset_index(name="count")
    top = agg.groupby("sport_name")["count"].sum().nlargest(8).index
    agg = agg[agg["sport_name"].isin(top)]
    fig = px.bar(agg, x="sport_name", y="count", color="medal_type",
                 color_discrete_map=MEDAL_COLORS, barmode="stack")
    fig.update_xaxes(tickangle=-30)
    return dark(fig, 280)


@app.callback(Output("tbl","data"),
              Input("f-season","value"), Input("f-year","value"), Input("f-medal","value"))
def table(seasons, yr, medals):
    d = filt(seasons, yr, medals)
    cols = ["athlete_name","country_name","sport_name","event_name","year","medal_type"]
    return d[cols].head(500).to_dict("records")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)