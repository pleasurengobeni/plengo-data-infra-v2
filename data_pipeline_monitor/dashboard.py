import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import os
import time
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import timedelta

# Load environment variables (.env file for local dev; Docker injects them directly)
load_dotenv()


def _build_engine():
    """
    Connect to the target DB where pipeline.etl_metrics lives.
    Reads DB_* env vars (set in docker-compose / .env); falls back to
    the cenfri_prd_con RDS defaults for local dev.
    """
    # Connect directly to the target DB where pipeline.etl_metrics lives
    db_user  = os.getenv("DB_USER",  "cenfri_db_admin")
    db_pass  = os.getenv("DB_PASS",  "y5j9p4aijrU2V6B9")
    db_host  = os.getenv("DB_HOST",  "cenfri-aws.cxc8802a0cus.af-south-1.rds.amazonaws.com")
    db_port  = int(os.getenv("DB_PORT", "5432"))
    db_name  = os.getenv("DB_NAME",  "cenfri_prod")

    url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    return create_engine(url, pool_pre_ping=True)


engine = _build_engine()

# SQL Query
query = """
SELECT 
    COALESCE(NULLIF(schema, ''), dag_id) AS source_system,
    dag_id,
    table_name, 
    created_at, 
    target_today_records,
    target_total_records,
    source_total_records,
    target_max_upload_date,
    target_max_incremental,
    source_max_incremental,
    source_loaded_to_max_target,
    rows_loaded_now,
    perc_loaded,
    run_id,
    run_start,
    run_end,
    run_duration_secs,
    run_status,
    error_msg,
    batch_size,
    COALESCE(engine_type, 'pandas') AS engine_type,
    COALESCE(source_type, 'db')     AS source_type
FROM pipeline.etl_metrics
WHERE created_at >= now() - interval '7 days'
ORDER BY created_at DESC
"""

def format_timedelta(hours):
    """Convert hours to human-readable format (days or hours:minutes)"""
    if pd.isna(hours):
        return "N/A"
    
    total_seconds = int(hours * 3600)
    days = total_seconds // (24 * 3600)
    remaining_seconds = total_seconds % (24 * 3600)
    
    if days > 0:
        return f"{days} day{'s' if days > 1 else ''}"
    else:
        hours = remaining_seconds // 3600
        minutes = (remaining_seconds % 3600) // 60
        if hours > 0:
            return f"{hours}h {minutes}m" if minutes > 0 else f"{hours}h"
        else:
            return f"{minutes}m"

# Initialize Dash app
app = dash.Dash(__name__, suppress_callback_exceptions=True)
server = app.server

# App layout
app.layout = html.Div([
    html.Div([
        html.H1("ETL Pipeline Monitoring Dashboard", style={'textAlign': 'center'}),
        html.Div([
            dcc.Interval(
                id='interval-component',
                interval=30 * 1000,  # refresh every 60 seconds
                n_intervals=0
            ),
        ], style={'textAlign': 'center', 'margin': '20px'}),
        
        dcc.Tabs([
            dcc.Tab(label='Summary View', children=[
                html.Div(id='summary-content')
            ]),
            dcc.Tab(label='Detailed Analytics', children=[
                html.Div(id='analytics-content')
            ]),
            dcc.Tab(label='Data Quality', children=[
                html.Div(id='quality-content')
            ]),
            dcc.Tab(label='Failures & Errors', children=[
                html.Div(id='failures-content')
            ])
        ])
    ], style={'fontFamily': 'Arial, sans-serif', 'padding': '20px'})
])

def format_duration(seconds):
    """Convert seconds to a human-readable string like 2m 35s."""
    if pd.isna(seconds) or seconds is None:
        return "N/A"
    seconds = int(seconds)
    if seconds >= 3600:
        h, r = divmod(seconds, 3600)
        m = r // 60
        return f"{h}h {m}m"
    elif seconds >= 60:
        m, s = divmod(seconds, 60)
        return f"{m}m {s}s"
    return f"{seconds}s"

_cache = {"data": None, "ts": 0}
CACHE_TTL = 25  # seconds — just under the 30s Interval so every tick gets fresh data

def load_and_process_data():
    now = time.monotonic()
    if _cache["data"] is not None and (now - _cache["ts"]) < CACHE_TTL:
        return _cache["data"]

    # Fetch data
    df = pd.read_sql_query(query, engine)
    
    if df.empty:
        _cache["data"] = (pd.DataFrame(), pd.DataFrame())
        _cache["ts"]   = time.monotonic()
        return pd.DataFrame(), pd.DataFrame()
    
    # Fill blank source_system so groupby never drops rows on NULL keys
    if 'dag_id' in df.columns:
        df['source_system'] = df['source_system'].where(df['source_system'].notna() & (df['source_system'] != ''), df['dag_id'])
    df['source_system'] = df['source_system'].fillna('unknown')

    # Convert datetime columns
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['target_max_upload_date'] = pd.to_datetime(df['target_max_upload_date']) + timedelta(hours=2)
    df['run_start'] = pd.to_datetime(df['run_start'])
    df['run_end']   = pd.to_datetime(df['run_end'])
    
    # Use DB-computed perc_loaded; derive perc_left from it
    df["perc_loaded"] = pd.to_numeric(df["perc_loaded"], errors="coerce")
    df["perc_left"]   = (100 - df["perc_loaded"]).clip(lower=0)
    df["table_label"] = df["source_system"] + "." + df["table_name"]
    df["duration_str"] = df["run_duration_secs"].apply(format_duration)
    
    # Calculate load intervals
    df_sorted = df.sort_values(['source_system', 'table_name', 'created_at'])
    df_sorted['load_diff'] = df_sorted.groupby(['source_system', 'table_name'])['created_at'].diff()
    df_sorted['load_diff_hrs'] = df_sorted['load_diff'].dt.total_seconds() / 3600
    
    # Get latest snapshot and average intervals
    df_latest = df_sorted.sort_values('created_at').groupby(['source_system', 'table_name'], as_index=False).last()
    avg_intervals = df_sorted.groupby(['source_system', 'table_name'])['load_diff_hrs'].mean().reset_index()
    avg_intervals.rename(columns={'load_diff_hrs': 'avg_load_interval_hrs'}, inplace=True)
    
    # Average run duration per table
    avg_duration = df_sorted.groupby(['source_system', 'table_name'])['run_duration_secs'].mean().reset_index()
    avg_duration.rename(columns={'run_duration_secs': 'avg_duration_secs'}, inplace=True)

    # Merge and format data
    df_latest = df_latest.merge(avg_intervals, on=['source_system', 'table_name'], how='left')
    df_latest = df_latest.merge(avg_duration,  on=['source_system', 'table_name'], how='left')
    df_latest['avg_load_interval_str'] = df_latest['avg_load_interval_hrs'].apply(format_timedelta)
    df_latest['avg_duration_str']      = df_latest['avg_duration_secs'].apply(format_duration)
    df_latest = df_latest.sort_values("rows_loaded_now", ascending=False)

    result = (df, df_latest)
    _cache["data"] = result
    _cache["ts"]   = time.monotonic()
    return result

@app.callback(
    Output('summary-content', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_summary(n_intervals):
    try:
        df, df_latest = load_and_process_data()
        
        if df.empty:
            return html.Div("No data available", style={'color': 'red', 'padding': '20px', 'textAlign': 'center'})
        
        # Create summary cards
        failed_today = int(df[df['run_status'] == 'failed']['table_name'].nunique()) if 'run_status' in df.columns else 0
        avg_duration_secs = df_latest['avg_duration_secs'].mean()
        overall_perc = df_latest['perc_loaded'].mean()
        spark_runs       = int((df['engine_type'] == 'spark').sum())  if 'engine_type' in df.columns else 0
        pandas_runs      = int((df['engine_type'] == 'pandas').sum()) if 'engine_type' in df.columns else 0
        file_runs        = int((df['source_type'] == 'file').sum())   if 'source_type' in df.columns else 0
        db_runs          = int((df['source_type'] != 'file').sum())   if 'source_type' in df.columns else len(df)
        spark_db_runs    = int(((df['engine_type'] == 'spark')  & (df['source_type'] != 'file')).sum()) if 'engine_type' in df.columns else 0
        spark_file_runs  = int(((df['engine_type'] == 'spark')  & (df['source_type'] == 'file')).sum()) if 'engine_type' in df.columns else 0
        pandas_file_runs = int(((df['engine_type'] == 'pandas') & (df['source_type'] == 'file')).sum()) if 'engine_type' in df.columns else 0
        pandas_db_runs   = int(((df['engine_type'] == 'pandas') & (df['source_type'] != 'file')).sum()) if 'engine_type' in df.columns else 0

        summary_cards = html.Div([
            # ── Row 1: top-level KPIs ────────────────────────────────────────
            html.Div([
                html.Div([
                    html.Div(f"{len(df_latest)}", style={'fontSize': '24px', 'fontWeight': 'bold'}),
                    html.Div("Tables Monitored")
                ], style={'padding': '20px', 'borderRadius': '5px', 'backgroundColor': '#f8f9fa',
                         'textAlign': 'center', 'boxShadow': '0 4px 8px 0 rgba(0,0,0,0.2)'}),

                html.Div([
                    html.Div(f"{df_latest['source_system'].nunique()}", style={'fontSize': '24px', 'fontWeight': 'bold'}),
                    html.Div("Source Systems")
                ], style={'padding': '20px', 'borderRadius': '5px', 'backgroundColor': '#f8f9fa',
                         'textAlign': 'center', 'boxShadow': '0 4px 8px 0 rgba(0,0,0,0.2)'}),

                html.Div([
                    html.Div(f"{df_latest['rows_loaded_now'].sum():,}", style={'fontSize': '24px', 'fontWeight': 'bold'}),
                    html.Div("Records Loaded (Last Run)")
                ], style={'padding': '20px', 'borderRadius': '5px', 'backgroundColor': '#f8f9fa',
                         'textAlign': 'center', 'boxShadow': '0 4px 8px 0 rgba(0,0,0,0.2)'}),

                html.Div([
                    html.Div(format_duration(avg_duration_secs), style={'fontSize': '24px', 'fontWeight': 'bold'}),
                    html.Div("Avg Run Duration")
                ], style={'padding': '20px', 'borderRadius': '5px', 'backgroundColor': '#e6f3ff',
                         'textAlign': 'center', 'boxShadow': '0 4px 8px 0 rgba(0,0,0,0.2)'}),

                html.Div([
                    html.Div(f"{failed_today}",
                             style={'fontSize': '24px', 'fontWeight': 'bold',
                                    'color': 'red' if failed_today > 0 else 'black'}),
                    html.Div("Tables With Failures")
                ], style={'padding': '20px', 'borderRadius': '5px',
                         'backgroundColor': '#ffcccc' if failed_today > 0 else '#f8f9fa',
                         'textAlign': 'center', 'boxShadow': '0 4px 8px 0 rgba(0,0,0,0.2)'}),

                html.Div([
                    html.Div(f"{overall_perc:.1f}%" if pd.notna(overall_perc) else "N/A",
                             style={'fontSize': '24px', 'fontWeight': 'bold'}),
                    html.Div("Avg Coverage (Source→Target)")
                ], style={'padding': '20px', 'borderRadius': '5px', 'backgroundColor': '#ccffcc',
                         'textAlign': 'center', 'boxShadow': '0 4px 8px 0 rgba(0,0,0,0.2)'}),
            ], style={'display': 'grid', 'gridTemplateColumns': 'repeat(6, 1fr)', 'gap': '20px', 'marginBottom': '16px'}),

            # ── Row 2: engine × source breakdown — Spark (left) | Pandas (right) ──
            html.Div([
                # ── Spark group ──────────────────────────────────────────────
                html.Div([
                    html.Div("⚡ Spark", style={
                        'fontWeight': 'bold', 'color': '#7b2d8b', 'fontSize': '13px',
                        'textTransform': 'uppercase', 'letterSpacing': '1px',
                        'marginBottom': '10px', 'borderBottom': '2px solid #7b2d8b', 'paddingBottom': '4px'
                    }),
                    html.Div([
                        html.Div([
                            html.Div(f"{spark_runs:,}", style={'fontSize': '28px', 'fontWeight': 'bold', 'color': '#7b2d8b'}),
                            html.Div("Total Runs (7d)", style={'fontSize': '12px', 'color': '#555'})
                        ], style={'padding': '16px', 'borderRadius': '5px', 'backgroundColor': '#f3e8ff',
                                  'textAlign': 'center', 'boxShadow': '0 2px 6px rgba(0,0,0,0.15)'}),

                        html.Div([
                            html.Div(f"{spark_db_runs:,}", style={'fontSize': '28px', 'fontWeight': 'bold', 'color': '#7b2d8b'}),
                            html.Div("🔗 DB-to-DB (7d)", style={'fontSize': '12px', 'color': '#555'})
                        ], style={'padding': '16px', 'borderRadius': '5px', 'backgroundColor': '#ede9fe',
                                  'textAlign': 'center', 'boxShadow': '0 2px 6px rgba(0,0,0,0.15)'}),

                        html.Div([
                            html.Div(f"{spark_file_runs:,}", style={'fontSize': '28px', 'fontWeight': 'bold', 'color': '#7b2d8b'}),
                            html.Div("📄 File-Based (7d)", style={'fontSize': '12px', 'color': '#555'})
                        ], style={'padding': '16px', 'borderRadius': '5px', 'backgroundColor': '#fdf4ff',
                                  'textAlign': 'center', 'boxShadow': '0 2px 6px rgba(0,0,0,0.15)'}),
                    ], style={'display': 'grid', 'gridTemplateColumns': 'repeat(3, 1fr)', 'gap': '12px'}),
                ], style={'padding': '16px', 'borderRadius': '8px', 'border': '1px solid #d8b4fe',
                          'backgroundColor': '#faf5ff'}),

                # ── Pandas group ─────────────────────────────────────────────
                html.Div([
                    html.Div("🐼 Pandas", style={
                        'fontWeight': 'bold', 'color': '#1a6ba0', 'fontSize': '13px',
                        'textTransform': 'uppercase', 'letterSpacing': '1px',
                        'marginBottom': '10px', 'borderBottom': '2px solid #1a6ba0', 'paddingBottom': '4px'
                    }),
                    html.Div([
                        html.Div([
                            html.Div(f"{pandas_runs:,}", style={'fontSize': '28px', 'fontWeight': 'bold', 'color': '#1a6ba0'}),
                            html.Div("Total Runs (7d)", style={'fontSize': '12px', 'color': '#555'})
                        ], style={'padding': '16px', 'borderRadius': '5px', 'backgroundColor': '#e8f4fd',
                                  'textAlign': 'center', 'boxShadow': '0 2px 6px rgba(0,0,0,0.15)'}),

                        html.Div([
                            html.Div(f"{pandas_db_runs:,}", style={'fontSize': '28px', 'fontWeight': 'bold', 'color': '#1a6ba0'}),
                            html.Div("🔗 DB-to-DB (7d)", style={'fontSize': '12px', 'color': '#555'})
                        ], style={'padding': '16px', 'borderRadius': '5px', 'backgroundColor': '#dbeafe',
                                  'textAlign': 'center', 'boxShadow': '0 2px 6px rgba(0,0,0,0.15)'}),

                        html.Div([
                            html.Div(f"{pandas_file_runs:,}", style={'fontSize': '28px', 'fontWeight': 'bold', 'color': '#166534'}),
                            html.Div("📄 File-Based (7d)", style={'fontSize': '12px', 'color': '#555'})
                        ], style={'padding': '16px', 'borderRadius': '5px', 'backgroundColor': '#f0fdf4',
                                  'textAlign': 'center', 'boxShadow': '0 2px 6px rgba(0,0,0,0.15)'}),
                    ], style={'display': 'grid', 'gridTemplateColumns': 'repeat(3, 1fr)', 'gap': '12px'}),
                ], style={'padding': '16px', 'borderRadius': '8px', 'border': '1px solid #bfdbfe',
                          'backgroundColor': '#f0f9ff'}),

            ], style={'display': 'grid', 'gridTemplateColumns': '1fr 1fr', 'gap': '20px', 'marginBottom': '30px'}),
        ])
        
        # Create summary table
        summary_table = dash_table.DataTable(
            id='summary-table',
            columns=[
                {"name": "Source System", "id": "source_system"},
                {"name": "Table Name", "id": "table_name"},
                {"name": "Pipeline", "id": "source_type"},
                {"name": "Status", "id": "run_status"},
                {"name": "Last Duration", "id": "duration_str"},
                {"name": "Avg Duration", "id": "avg_duration_str"},
                {"name": "Target Records", "id": "target_total_records", "type": "numeric", "format": {"specifier": ","}},
                {"name": "Source Records", "id": "source_total_records", "type": "numeric", "format": {"specifier": ","}},
                {"name": "Coverage %", "id": "perc_loaded", "type": "numeric", "format": {"specifier": ".2f"}},
                {"name": "% Left", "id": "perc_left", "type": "numeric", "format": {"specifier": ".2f"}},
                {"name": "Last Loaded", "id": "target_max_upload_date"},
                {"name": "Rows Loaded Last", "id": "rows_loaded_now", "type": "numeric", "format": {"specifier": ","}},
                {"name": "Avg Load Interval", "id": "avg_load_interval_str"},
                {"name": "Engine", "id": "engine_type"}
            ],
            data=df_latest.to_dict('records'),
            style_table={'overflowX': 'auto', 'marginBottom': '30px'},
            style_header={
                'backgroundColor': 'rgb(230, 230, 230)',
                'fontWeight': 'bold',
                'border': '1px solid black'
            },
            style_cell={
                'border': '1px solid grey',
                'textAlign': 'left',
                'padding': '5px'
            },
            style_data_conditional=[
                {
                    'if': {'filter_query': '{run_status} = "failed"', 'column_id': 'run_status'},
                    'backgroundColor': '#ffcccc', 'color': 'red', 'fontWeight': 'bold'
                },
                {
                    'if': {'filter_query': '{run_status} = "success"', 'column_id': 'run_status'},
                    'backgroundColor': '#ccffcc', 'color': 'green'
                },
                {
                    'if': {'filter_query': '{run_status} = "no_new_rows"', 'column_id': 'run_status'},
                    'backgroundColor': '#e8f4fd', 'color': '#1a6ba0'
                },
                {
                    'if': {
                        'filter_query': '{perc_left} > 10',
                        'column_id': 'perc_left'
                    },
                    'backgroundColor': '#ffcccc',
                    'color': 'black'
                },
                {
                    'if': {
                        'filter_query': '{perc_left} <= 10',
                        'column_id': 'perc_left'
                    },
                    'backgroundColor': '#ccffcc',
                    'color': 'black'
                },
                {
                    'if': {
                        'filter_query': '{rows_loaded_now} > 0',
                        'column_id': 'rows_loaded_now'
                    },
                    'backgroundColor': '#e6f3ff',
                    'color': 'black'
                },
                {
                    'if': {
                        'filter_query': '{target_max_incremental} < {source_max_incremental}',
                        'column_id': 'target_max_incremental'
                    },
                    'backgroundColor': '#fff3cd',
                    'color': 'black'
                },
                {
                    'if': {'filter_query': '{engine_type} = "spark"', 'column_id': 'engine_type'},
                    'backgroundColor': '#f3e8ff', 'color': '#7b2d8b', 'fontWeight': 'bold'
                },
                {
                    'if': {'filter_query': '{engine_type} = "pandas"', 'column_id': 'engine_type'},
                    'backgroundColor': '#e8f4fd', 'color': '#1a6ba0', 'fontWeight': 'bold'
                },
                # source_type column — green = file, blue = db
                {
                    'if': {'filter_query': '{source_type} = "file"', 'column_id': 'source_type'},
                    'backgroundColor': '#f0fdf4', 'color': '#166534', 'fontWeight': 'bold'
                },
                {
                    'if': {'filter_query': '{source_type} = "db"', 'column_id': 'source_type'},
                    'backgroundColor': '#eff6ff', 'color': '#1e40af', 'fontWeight': 'bold'
                }
            ]
        )


        
        # ── Group-view sub-tables ─────────────────────────────────────────
        def _group_table(table_id, subset_df, title, header_color):
            """Render a smaller DataTable for one engine×source group."""
            if subset_df.empty:
                return html.Div()
            return html.Div([
                html.H3(title, style={'marginTop': '24px', 'color': header_color}),
                dash_table.DataTable(
                    id=table_id,
                    columns=[
                        {"name": "Source System",  "id": "source_system"},
                        {"name": "Table Name",      "id": "table_name"},
                        {"name": "Status",          "id": "run_status"},
                        {"name": "Last Duration",   "id": "duration_str"},
                        {"name": "Avg Duration",    "id": "avg_duration_str"},
                        {"name": "Target Records",  "id": "target_total_records",  "type": "numeric", "format": {"specifier": ","}},
                        {"name": "Source Records",  "id": "source_total_records",  "type": "numeric", "format": {"specifier": ","}},
                        {"name": "Coverage %",      "id": "perc_loaded",           "type": "numeric", "format": {"specifier": ".2f"}},
                        {"name": "% Left",          "id": "perc_left",             "type": "numeric", "format": {"specifier": ".2f"}},
                        {"name": "Last Loaded",     "id": "target_max_upload_date"},
                        {"name": "Rows Loaded Last","id": "rows_loaded_now",        "type": "numeric", "format": {"specifier": ","}},
                        {"name": "Avg Load Interval","id": "avg_load_interval_str"},
                        {"name": "Engine",          "id": "engine_type"},
                    ],
                    data=subset_df.to_dict('records'),
                    style_table={'overflowX': 'auto', 'marginBottom': '20px'},
                    style_header={'backgroundColor': 'rgb(230,230,230)', 'fontWeight': 'bold', 'border': '1px solid black'},
                    style_cell={'border': '1px solid grey', 'textAlign': 'left', 'padding': '5px'},
                    style_data_conditional=[
                        {'if': {'filter_query': '{run_status} = "failed"',     'column_id': 'run_status'}, 'backgroundColor': '#ffcccc', 'color': 'red',     'fontWeight': 'bold'},
                        {'if': {'filter_query': '{run_status} = "success"',    'column_id': 'run_status'}, 'backgroundColor': '#ccffcc', 'color': 'green'},
                        {'if': {'filter_query': '{run_status} = "no_new_rows"','column_id': 'run_status'}, 'backgroundColor': '#e8f4fd', 'color': '#1a6ba0'},
                        {'if': {'filter_query': '{perc_left} > 10',            'column_id': 'perc_left'},  'backgroundColor': '#ffcccc'},
                        {'if': {'filter_query': '{perc_left} <= 10',           'column_id': 'perc_left'},  'backgroundColor': '#ccffcc'},
                        {'if': {'filter_query': '{rows_loaded_now} > 0',       'column_id': 'rows_loaded_now'}, 'backgroundColor': '#e6f3ff'},
                        {'if': {'filter_query': '{engine_type} = "spark"',     'column_id': 'engine_type'}, 'backgroundColor': '#f3e8ff', 'color': '#7b2d8b', 'fontWeight': 'bold'},
                        {'if': {'filter_query': '{engine_type} = "pandas"',    'column_id': 'engine_type'}, 'backgroundColor': '#e8f4fd', 'color': '#1a6ba0', 'fontWeight': 'bold'},
                    ]
                )
            ])

        df_spark_db    = df_latest[(df_latest['engine_type'] == 'spark')  & (df_latest['source_type'] != 'file')]
        df_spark_file  = df_latest[(df_latest['engine_type'] == 'spark')  & (df_latest['source_type'] == 'file')]
        df_pandas_db   = df_latest[(df_latest['engine_type'] == 'pandas') & (df_latest['source_type'] != 'file')]
        df_pandas_file = df_latest[(df_latest['engine_type'] == 'pandas') & (df_latest['source_type'] == 'file')]

        group_section = html.Div([
            html.H2("Group Views", style={'marginTop': '30px'}),
            html.Div([
                html.Div([
                    _group_table('grp-spark-db',   df_spark_db,   "⚡🔗 Spark — DB-to-DB",     '#7b2d8b'),
                    _group_table('grp-spark-file', df_spark_file, "⚡📄 Spark — File",         '#9d4edd'),
                ], style={'display': 'grid', 'gridTemplateColumns': '1fr 1fr', 'gap': '20px'}),
                html.Div([
                    _group_table('grp-pandas-db',   df_pandas_db,   "🐼🔗 Pandas — DB-to-DB",  '#1a6ba0'),
                    _group_table('grp-pandas-file', df_pandas_file, "🐼📄 Pandas — File",      '#166534'),
                ], style={'display': 'grid', 'gridTemplateColumns': '1fr 1fr', 'gap': '20px'}),
            ])
        ])

        # Engine type breakdown pie chart
        engine_counts = df.groupby('engine_type').size().reset_index(name='runs')
        engine_pie = dcc.Graph(
            figure=px.pie(
                engine_counts,
                names='engine_type',
                values='runs',
                title='Run Engine Distribution (7 days)',
                color='engine_type',
                color_discrete_map={'spark': '#7b2d8b', 'pandas': '#1a6ba0'},
                hole=0.4
            ).update_traces(
                textinfo='label+percent+value',
                hovertemplate='<b>%{label}</b><br>Runs: %{value}<br>%{percent}<extra></extra>'
            )
        )

        # Engine type bar by table (latest run engine per table)
        engine_bar = dcc.Graph(
            figure=px.bar(
                df_latest,
                x='table_label',
                y='rows_loaded_now',
                color='engine_type',
                title='Rows Loaded Last Run — by Engine',
                color_discrete_map={'spark': '#7b2d8b', 'pandas': '#1a6ba0'},
                labels={'rows_loaded_now': 'Rows Loaded', 'table_label': 'Table', 'engine_type': 'Engine'}
            ).update_layout(xaxis_title='', yaxis_title='Rows Loaded')
        )

        # Create interval chart
        interval_chart = dcc.Graph(
            figure=px.bar(
                df_latest,
                x="table_label",
                y="avg_load_interval_hrs",
                color='engine_type',
                color_discrete_map={'spark': '#7b2d8b', 'pandas': '#1a6ba0'},
                title="Average Load Intervals by Table",
                labels={
                    'avg_load_interval_hrs': 'Hours',
                    'table_label': 'Table',
                    'engine_type': 'Engine'
                },
                hover_data={
                    'avg_load_interval_str': True,
                    'avg_load_interval_hrs': False
                },
                custom_data=['avg_load_interval_str']
            ).update_traces(
                hovertemplate="<b>%{x}</b><br>Avg Load Interval: %{customdata[0]}<extra></extra>"
            ).update_layout(
                yaxis_title="Average Interval (hours)",
                xaxis_title="",
                hovermode="closest"
            )
        )
        
        # Run duration bar chart
        duration_chart = dcc.Graph(
            figure=px.bar(
                df_latest,
                x="table_label",
                y="avg_duration_secs",
                color='engine_type',
                color_discrete_map={'spark': '#7b2d8b', 'pandas': '#1a6ba0'},
                title="Average DAG Run Duration by Table",
                labels={
                    'avg_duration_secs': 'Seconds',
                    'table_label': 'Table',
                    'engine_type': 'Engine'
                },
                hover_data={'avg_duration_str': True, 'avg_duration_secs': False},
                custom_data=['avg_duration_str']
            ).update_traces(
                hovertemplate="<b>%{x}</b><br>Avg Duration: %{customdata[0]}<extra></extra>"
            ).update_layout(
                yaxis_title="Avg Duration (seconds)",
                xaxis_title=""
            )
        )

        # Duration trend over time (scatter)
        duration_trend = dcc.Graph(
            figure=px.scatter(
                df.sort_values("created_at"),
                x="created_at",
                y="run_duration_secs",
                color="engine_type",
                color_discrete_map={'spark': '#7b2d8b', 'pandas': '#1a6ba0'},
                symbol="run_status",
                hover_data=["run_id", "table_label", "rows_loaded_now", "duration_str"],
                title="Run Duration Over Time (by Engine)",
                labels={
                    'run_duration_secs': 'Duration (s)',
                    'created_at': 'Run Timestamp',
                    'engine_type': 'Engine',
                    'run_status': 'Status'
                }
            ).update_layout(
                xaxis_title="Time",
                yaxis_title="Duration (seconds)"
            )
        )

        return html.Div([
            summary_cards,
            html.H2("Engine Breakdown", style={'marginTop': '20px'}),
            html.Div([engine_pie, engine_bar],
                     style={'display': 'grid', 'gridTemplateColumns': '1fr 2fr', 'gap': '20px', 'marginBottom': '20px'}),
            html.H2("ETL Status Summary"),
            summary_table,
            group_section,
            html.H2("Load Frequency Analysis"),
            interval_chart,
            html.H2("Run Duration Analysis"),
            duration_chart,
            html.H2("Run Duration Trend (7 days)"),
            duration_trend,
        ])
    
    except Exception as e:
        return html.Div(f"Error loading data: {str(e)}", style={'color': 'red', 'padding': '20px', 'textAlign': 'center'})

@app.callback(
    Output('analytics-content', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_analytics(n_intervals):
    try:
        df, df_latest = load_and_process_data()
        
        if df.empty:
            return html.Div("No data available", style={'color': 'red', 'padding': '20px', 'textAlign': 'center'})
        
        # Create analytics visualizations
        return html.Div([
            html.H2("Records Loaded Today"),
            dcc.Graph(
                figure=px.bar(
                    df_latest,
                    x='table_label',
                    y='target_today_records',
                    color='engine_type',
                    color_discrete_map={'spark': '#7b2d8b', 'pandas': '#1a6ba0'},
                    title='Records Loaded by Table (Today)',
                    labels={
                        'target_today_records': 'Records Loaded',
                        'table_label': 'Table',
                        'engine_type': 'Engine'
                    }
                ).update_layout(
                    xaxis_title="",
                    yaxis_title="Records Loaded"
                )
            ),
            
            html.H2("Loading Progress Over Time"),
            dcc.Graph(
                figure=px.line(
                    df.sort_values("created_at"),
                    x="created_at",
                    y="target_today_records",
                    color="table_label",
                    line_dash="engine_type",
                    line_dash_map={'spark': 'dash', 'pandas': 'solid'},
                    title="Daily Records Loaded Over Time (solid=pandas, dashed=spark)",
                    labels={
                        'created_at': 'Timestamp',
                        'target_today_records': 'Records Loaded',
                        'table_label': 'Table',
                        'engine_type': 'Engine'
                    }
                ).update_layout(
                    xaxis_title="Time",
                    yaxis_title="Records Loaded"
                )
            ),
            
            html.H2("Completion Status"),
            dcc.Graph(
                figure=px.bar(
                    df_latest,
                    x="table_label",
                    y="perc_left",
                    color='engine_type',
                    color_discrete_map={'spark': '#7b2d8b', 'pandas': '#1a6ba0'},
                    title="Percentage of Records Remaining to Load",
                    labels={
                        'perc_left': '% Remaining',
                        'table_label': 'Table',
                        'engine_type': 'Engine'
                    }
                ).update_layout(
                    yaxis_title="Percentage Remaining",
                    xaxis_title=""
                )
            )
        ])
    
    except Exception as e:
        return html.Div(f"Error loading data: {str(e)}", style={'color': 'red', 'padding': '20px', 'textAlign': 'center'})

# ── SQL helpers for Data Quality tab ────────────────────────────────────────
_DQ_SCHEMA_QUERY = """
    SELECT DISTINCT
        COALESCE(NULLIF(schema, ''), dag_id) AS source_system,
        table_name,
        COALESCE(NULLIF(schema, ''), dag_id) AS target_schema
    FROM pipeline.etl_metrics
    WHERE source_type = 'db'
      AND created_at >= now() - interval '7 days'
"""

_HWM_QUERY = """
    SELECT
        COALESCE(NULLIF(schema, ''), dag_id)    AS source_system,
        table_name,
        COALESCE(engine_type, 'pandas')         AS engine_type,
        target_max_incremental,
        source_loaded_to_max_target,
        target_total_records,
        source_total_records,
        perc_loaded,
        run_status
    FROM pipeline.etl_metrics
    WHERE created_at >= now() - interval '7 days'
      AND target_max_incremental IS NOT NULL
      AND source_loaded_to_max_target IS NOT NULL
    ORDER BY created_at DESC
"""


def _latest_hwm_per_table(df_hwm):
    """Return one row per (source_system, table_name) — the most recent run."""
    return (
        df_hwm
        .sort_values("created_at", ascending=False)
        .groupby(["source_system", "table_name"], as_index=False)
        .first()
    )


@app.callback(
    Output('quality-content', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_quality(n_intervals):
    try:
        # ── 1. HWM Coverage Check ────────────────────────────────────────────
        hwm_query = """
            SELECT
                COALESCE(NULLIF(schema, ''), dag_id) AS source_system,
                table_name,
                COALESCE(engine_type, 'pandas')      AS engine_type,
                target_max_incremental,
                source_loaded_to_max_target,
                target_total_records,
                source_total_records,
                perc_loaded,
                run_status,
                created_at
            FROM pipeline.etl_metrics
            WHERE created_at >= now() - interval '7 days'
              AND target_max_incremental IS NOT NULL
              AND source_loaded_to_max_target IS NOT NULL
            ORDER BY created_at DESC
        """
        df_hwm = pd.read_sql_query(hwm_query, engine)

        hwm_section = html.Div(
            "No HWM data available (DB pipelines only).",
            style={'color': '#888', 'padding': '12px'}
        )

        if not df_hwm.empty:
            df_hwm['created_at'] = pd.to_datetime(df_hwm['created_at'])
            # One row per table — latest run
            df_latest_hwm = (
                df_hwm
                .sort_values('created_at', ascending=False)
                .groupby(['source_system', 'table_name'], as_index=False)
                .first()
            )
            df_latest_hwm['table_label'] = (
                df_latest_hwm['source_system'] + '.' + df_latest_hwm['table_name']
            )

            # Melt into long form for side-by-side bars
            df_melt = df_latest_hwm.melt(
                id_vars=['table_label', 'engine_type', 'target_max_incremental'],
                value_vars=['source_loaded_to_max_target', 'target_total_records'],
                var_name='count_type',
                value_name='count'
            )
            df_melt['count_type'] = df_melt['count_type'].map({
                'source_loaded_to_max_target': 'Source rows ≤ max target HWM',
                'target_total_records':        'Target row count',
            })

            hwm_bar = dcc.Graph(
                figure=px.bar(
                    df_melt,
                    x='table_label',
                    y='count',
                    color='count_type',
                    barmode='group',
                    color_discrete_map={
                        'Source rows ≤ max target HWM': '#f59e0b',
                        'Target row count':              '#3b82f6',
                    },
                    title='HWM Coverage: Source rows ≤ max target HWM  vs  Target row count',
                    labels={
                        'count':       'Row Count',
                        'table_label': 'Table',
                        'count_type':  'Metric',
                    },
                    hover_data=['target_max_incremental'],
                ).update_layout(
                    xaxis_title='',
                    yaxis_title='Rows',
                    legend_title='',
                    hovermode='x unified',
                )
            )

            # Traffic-light summary table
            df_latest_hwm['gap'] = (
                pd.to_numeric(df_latest_hwm['source_loaded_to_max_target'], errors='coerce')
                - pd.to_numeric(df_latest_hwm['target_total_records'], errors='coerce')
            )
            df_latest_hwm['hwm_match'] = df_latest_hwm['gap'].apply(
                lambda g: '✅ Match' if pd.notna(g) and abs(g) == 0
                          else ('⚠️ Gap: {:,}'.format(int(g)) if pd.notna(g) else '—')
            )

            hwm_table = dash_table.DataTable(
                id='hwm-check-table',
                columns=[
                    {'name': 'Source System',              'id': 'source_system'},
                    {'name': 'Table',                      'id': 'table_name'},
                    {'name': 'Engine',                     'id': 'engine_type'},
                    {'name': 'Max Target HWM',             'id': 'target_max_incremental'},
                    {'name': 'Src rows ≤ HWM',             'id': 'source_loaded_to_max_target',
                     'type': 'numeric', 'format': {'specifier': ','}},
                    {'name': 'Target rows',                'id': 'target_total_records',
                     'type': 'numeric', 'format': {'specifier': ','}},
                    {'name': 'Coverage %',                 'id': 'perc_loaded',
                     'type': 'numeric', 'format': {'specifier': '.2f'}},
                    {'name': 'HWM Check',                  'id': 'hwm_match'},
                    {'name': 'Status',                     'id': 'run_status'},
                ],
                data=df_latest_hwm.to_dict('records'),
                style_table={'overflowX': 'auto', 'marginBottom': '20px'},
                style_header={'backgroundColor': 'rgb(230,230,230)', 'fontWeight': 'bold', 'border': '1px solid black'},
                style_cell={'border': '1px solid grey', 'textAlign': 'left', 'padding': '5px'},
                style_data_conditional=[
                    {'if': {'filter_query': '{hwm_match} contains "Match"', 'column_id': 'hwm_match'},
                     'backgroundColor': '#ccffcc', 'color': '#166534', 'fontWeight': 'bold'},
                    {'if': {'filter_query': '{hwm_match} contains "Gap"', 'column_id': 'hwm_match'},
                     'backgroundColor': '#fff3cd', 'color': '#92400e', 'fontWeight': 'bold'},
                    {'if': {'filter_query': '{run_status} = "failed"', 'column_id': 'run_status'},
                     'backgroundColor': '#ffcccc', 'color': 'red', 'fontWeight': 'bold'},
                    {'if': {'filter_query': '{run_status} = "success"', 'column_id': 'run_status'},
                     'backgroundColor': '#ccffcc', 'color': 'green'},
                ],
            )

            hwm_section = html.Div([hwm_bar, hwm_table])

        # ── 2. Duplicate Check (per target table, _row_checksum) ─────────────
        # Discover which tables exist and have _row_checksum (DB pipelines only)
        schema_query = """
            SELECT DISTINCT
                COALESCE(NULLIF(schema, ''), split_part(dag_id, '_', 2)) AS target_schema,
                table_name
            FROM pipeline.etl_metrics
            WHERE source_type = 'db'
              AND created_at >= now() - interval '7 days'
        """
        df_tables = pd.read_sql_query(schema_query, engine)

        dup_rows = []
        dup_errors = []
        for _, row in df_tables.iterrows():
            tgt_schema = row['target_schema']
            tgt_table  = row['table_name']
            try:
                dup_sql = f"""
                    SELECT
                        '{tgt_schema}'  AS target_schema,
                        '{tgt_table}'   AS table_name,
                        COUNT(*) FILTER (WHERE dup_count > 1) AS duplicate_rows,
                        SUM(dup_count)  FILTER (WHERE dup_count > 1) - 
                        COUNT(*) FILTER (WHERE dup_count > 1)        AS extra_copies,
                        COUNT(*) AS total_distinct_checksums,
                        MAX(dup_count) AS max_copies
                    FROM (
                        SELECT _row_checksum, COUNT(*) AS dup_count
                        FROM {tgt_schema}.{tgt_table}
                        GROUP BY _row_checksum
                    ) _chk
                """
                row_data = pd.read_sql_query(dup_sql, engine)
                dup_rows.append(row_data)
            except Exception as ex:
                # Table may not have _row_checksum yet (pre-migration) — skip silently
                dup_errors.append(f"{tgt_schema}.{tgt_table}: {ex}")

        dup_section = html.Div(
            "No target tables with _row_checksum found (run at least one DB pipeline first).",
            style={'color': '#888', 'padding': '12px'}
        )

        if dup_rows:
            df_dup = pd.concat(dup_rows, ignore_index=True)
            df_dup['table_label'] = df_dup['target_schema'] + '.' + df_dup['table_name']
            df_dup['duplicate_rows'] = pd.to_numeric(df_dup['duplicate_rows'], errors='coerce').fillna(0).astype(int)
            df_dup['extra_copies']   = pd.to_numeric(df_dup['extra_copies'],   errors='coerce').fillna(0).astype(int)
            df_dup['max_copies']     = pd.to_numeric(df_dup['max_copies'],     errors='coerce').fillna(0).astype(int)
            df_dup['status']         = df_dup['duplicate_rows'].apply(
                lambda d: '✅ No duplicates' if d == 0 else f'⚠️ {d:,} dup checksum(s)'
            )

            dup_bar = dcc.Graph(
                figure=px.bar(
                    df_dup,
                    x='table_label',
                    y='duplicate_rows',
                    color='duplicate_rows',
                    color_continuous_scale=[
                        [0,   '#22c55e'],   # green  — no dups
                        [0.01,'#f59e0b'],   # amber  — a few
                        [1,   '#ef4444'],   # red    — lots
                    ],
                    title='Duplicate Row Checksums per Target Table',
                    labels={
                        'duplicate_rows': 'Rows with duplicate checksum',
                        'table_label':    'Table',
                    },
                    hover_data=['extra_copies', 'max_copies', 'total_distinct_checksums'],
                ).update_layout(
                    xaxis_title='',
                    yaxis_title='Duplicate checksum rows',
                    coloraxis_showscale=False,
                )
            )

            dup_table = dash_table.DataTable(
                id='dup-check-table',
                columns=[
                    {'name': 'Schema',                    'id': 'target_schema'},
                    {'name': 'Table',                     'id': 'table_name'},
                    {'name': 'Distinct Checksums',        'id': 'total_distinct_checksums',
                     'type': 'numeric', 'format': {'specifier': ','}},
                    {'name': 'Dup Checksum rows',         'id': 'duplicate_rows',
                     'type': 'numeric', 'format': {'specifier': ','}},
                    {'name': 'Extra copies',              'id': 'extra_copies',
                     'type': 'numeric', 'format': {'specifier': ','}},
                    {'name': 'Max copies of one row',     'id': 'max_copies',
                     'type': 'numeric', 'format': {'specifier': ','}},
                    {'name': 'Duplicate Status',          'id': 'status'},
                ],
                data=df_dup.to_dict('records'),
                style_table={'overflowX': 'auto', 'marginBottom': '20px'},
                style_header={'backgroundColor': 'rgb(230,230,230)', 'fontWeight': 'bold', 'border': '1px solid black'},
                style_cell={'border': '1px solid grey', 'textAlign': 'left', 'padding': '5px'},
                style_data_conditional=[
                    {'if': {'filter_query': '{duplicate_rows} = 0', 'column_id': 'status'},
                     'backgroundColor': '#ccffcc', 'color': '#166534', 'fontWeight': 'bold'},
                    {'if': {'filter_query': '{duplicate_rows} > 0', 'column_id': 'status'},
                     'backgroundColor': '#fff3cd', 'color': '#92400e', 'fontWeight': 'bold'},
                    {'if': {'filter_query': '{duplicate_rows} > 0', 'column_id': 'duplicate_rows'},
                     'backgroundColor': '#fff3cd'},
                ]
            )

            dup_section = html.Div([dup_bar, dup_table])

            if dup_errors:
                dup_section = html.Div([
                    dup_section,
                    html.Details([
                        html.Summary(f"{len(dup_errors)} table(s) skipped (no _row_checksum column yet)",
                                     style={'color': '#888', 'cursor': 'pointer'}),
                        html.Ul([html.Li(e, style={'fontSize': '12px', 'color': '#888'}) for e in dup_errors])
                    ], style={'marginTop': '8px'})
                ])

        return html.Div([
            html.H2("HWM Coverage Check",
                    style={'marginTop': '10px', 'color': '#1e3a5f'}),
            html.P(
                "Compares source rows where incremental_column ≤ max target HWM "
                "against the target row count. A gap means rows were missed during load.",
                style={'color': '#555', 'marginBottom': '8px'}
            ),
            hwm_section,
            html.Hr(),
            html.H2("Duplicate Row Check",
                    style={'marginTop': '24px', 'color': '#1e3a5f'}),
            html.P(
                "Groups target table rows by _row_checksum. Any checksum appearing "
                "more than once indicates an exact duplicate row.",
                style={'color': '#555', 'marginBottom': '8px'}
            ),
            dup_section,
        ])

    except Exception as e:
        import traceback
        return html.Div([
            html.Div(f"Error loading Data Quality tab: {str(e)}",
                     style={'color': 'red', 'padding': '20px'}),
            html.Pre(traceback.format_exc(), style={'fontSize': '11px', 'color': '#888'})
        ])


@app.callback(
    Output('failures-content', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_failures(n_intervals):
    try:
        df, df_latest = load_and_process_data()

        if df.empty:
            return html.Div("No data available", style={'color': 'red', 'padding': '20px', 'textAlign': 'center'})

        df_failed = df[df['run_status'] == 'failed'].copy() if 'run_status' in df.columns else pd.DataFrame()

        if df_failed.empty:
            no_failures = html.Div(
                "✅ No failures in the last 7 days.",
                style={'color': 'green', 'fontSize': '20px', 'padding': '30px', 'textAlign': 'center'}
            )
            failures_section = no_failures
        else:
            failures_table = dash_table.DataTable(
                id='failures-table',
                columns=[
                    {"name": "DAG Run ID",       "id": "run_id"},
                    {"name": "Source System",     "id": "source_system"},
                    {"name": "Table",             "id": "table_name"},
                    {"name": "Engine",            "id": "engine_type"},
                    {"name": "Status",            "id": "run_status"},
                    {"name": "Error",             "id": "error_msg"},
                    {"name": "Run Start",         "id": "run_start"},
                    {"name": "Duration",          "id": "duration_str"},
                    {"name": "Rows Attempted",    "id": "rows_loaded_now", "type": "numeric", "format": {"specifier": ","}},
                ],
                data=df_failed.to_dict('records'),
                style_table={'overflowX': 'auto', 'marginBottom': '30px'},
                style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold', 'border': '1px solid black'},
                style_cell={'border': '1px solid grey', 'textAlign': 'left', 'padding': '5px'},
                style_data_conditional=[
                    {'if': {'column_id': 'run_status'}, 'backgroundColor': '#ffcccc', 'color': 'red', 'fontWeight': 'bold'}
                ]
            )

            fail_trend = dcc.Graph(
                figure=px.histogram(
                    df_failed,
                    x="created_at",
                    color="table_label",
                    nbins=14,
                    title="Failure Frequency Over Time (7 days)",
                    labels={'created_at': 'Date', 'count': 'Failures', 'table_label': 'Table'}
                ).update_layout(xaxis_title="Date", yaxis_title="Failure Count")
            )
            failures_section = html.Div([failures_table, fail_trend])

        # Slowest runs table (top 10)
        df_slow = df.nlargest(10, 'run_duration_secs')[
            ['source_system', 'table_name', 'engine_type', 'run_id', 'run_start', 'run_duration_secs', 'duration_str', 'rows_loaded_now', 'run_status']
        ].copy()

        slow_table = dash_table.DataTable(
            id='slow-runs-table',
            columns=[
                {"name": "Source System",  "id": "source_system"},
                {"name": "Table",          "id": "table_name"},
                {"name": "Engine",         "id": "engine_type"},
                {"name": "Run ID",         "id": "run_id"},
                {"name": "Run Start",      "id": "run_start"},
                {"name": "Duration",       "id": "duration_str"},
                {"name": "Rows Loaded",    "id": "rows_loaded_now", "type": "numeric", "format": {"specifier": ","}},
                {"name": "Status",         "id": "run_status"},
            ],
            data=df_slow.to_dict('records'),
            style_table={'overflowX': 'auto', 'marginBottom': '30px'},
            style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold', 'border': '1px solid black'},
            style_cell={'border': '1px solid grey', 'textAlign': 'left', 'padding': '5px'},
        )

        return html.Div([
            html.H2("Recent Failures (7 days)"),
            failures_section,
            html.H2("Top 10 Slowest Runs (7 days)"),
            slow_table,
        ])

    except Exception as e:
        return html.Div(f"Error loading data: {str(e)}", style={'color': 'red', 'padding': '20px', 'textAlign': 'center'})


# Run app
if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=8050)