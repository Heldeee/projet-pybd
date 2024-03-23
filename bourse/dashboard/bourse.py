import dash
from dash import dcc, html, dash_table
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
import plotly.express as px
import datetime as dt

import logging

LOG = logging.getLogger(__name__)

# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

DATABASE_URI = 'timescaledb://ricou:monmdp@db:5432/bourse'    # inside docker
# DATABASE_URI = 'timescaledb://ricou:monmdp@localhost:5432/bourse'  # outisde docker
engine = sqlalchemy.create_engine(DATABASE_URI)

app = dash.Dash(__name__,  title="Bourse", suppress_callback_exceptions=True) # , external_stylesheets=external_stylesheets)
server = app.server

# Search bar with smart search (dropdown)
companies = pd.read_sql_query("SELECT * FROM companies", engine)
company_options = [{'label': row['company_name'], 'value': row['cid']} for index, row in companies.iterrows()]

app.layout = html.Div([
                dcc.Textarea(
                    id='sql-query',
                    value='''
                        SELECT * FROM pg_catalog.pg_tables
                            WHERE schemaname != 'pg_catalog' AND 
                                  schemaname != 'information_schema';
                    ''',
                    style={'width': '100%', 'height': 100},
                    ),
                html.Button('Execute', id='execute-query', n_clicks=0),
                html.Div(id='query-result'),


                dcc.Dropdown(id='company-dropdown',
                    options= company_options,
                    multi=True,
                    placeholder='Select companies...'
                ),
                dcc.RadioItems(
                    id='graph-type',
                    options=[
                        {'label': 'Line', 'value': 'line'},
                        {'label': 'Candlestick', 'value': 'candlestick'}
                    ],
                    value='line',
                    labelStyle={'display': 'inline-block'}
                ),

                dcc.DatePickerRange(
                    id='date-picker-range',
                    min_date_allowed=dt.datetime(2019, 1, 1),
                    max_date_allowed=dt.datetime.now(),
                    initial_visible_month=dt.datetime.now(),
                    start_date=dt.datetime(2019, 1, 1),
                    end_date=dt.datetime.now()
                ),
                dcc.Input(id='company-id-input', type='number', placeholder='Enter company ID...'),
                dcc.Graph(id='stock-graph')
             ])


@app.callback( ddep.Output('query-result', 'children'),
               ddep.Input('execute-query', 'n_clicks'),
               ddep.State('sql-query', 'value'),
             )
def run_query(n_clicks, query):
    if n_clicks > 0:
        try:
            result_df = pd.read_sql_query(query, engine)
            LOG.info(f"dataframe: {result_df}")
            return html.Pre(result_df.to_string())
        except Exception as e:
            return html.Pre(str(e))
    return "Enter a query and press execute."

@app.callback(
    ddep.Output('stock-graph', 'figure'),
    [ddep.Input('company-id-input', 'value')]
)
def update_graph(company_id):
    if company_id is None:
        return {}
    # Requête SQL pour récupérer les données de l'entreprise spécifique
    query = f"""
        SELECT date, value
        FROM stocks
        WHERE cid = {company_id}
        ORDER BY date
    """


    # Charger les données directement en DataFrame Pandas en utilisant la requête SQL
    df = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date'])
    
    # Créer un graphe à partir des données en utilisant Plotly Express
    fig = px.line(df, x=df.index, y='value', labels={'x': 'Date', 'y': 'Prix de l\'action'}, 
                  title='Évolution du prix de l\'action pour une entreprise spécifique')
    
    return fig

if __name__ == '__main__':
    app.run(debug=True)
