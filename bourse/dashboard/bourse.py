import dash
from dash import dcc, html, dash_table
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
import plotly.express as px
import plotly.graph_objects as go
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
companies_options = [{'label': row['name'], 'value': row['id']} for index, row in companies.iterrows()]

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
                    multi=False,
                    options=companies_options,
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
    [ddep.Input('company-dropdown', 'value'),
     ddep.Input('date-picker-range', 'start_date'),
     ddep.Input('date-picker-range', 'end_date'),
     ddep.Input('graph-type', 'value')]
)
def update_graph(company_id, start_date, end_date, graph_type='line'):
    if company_id is None:
        return {}

    start_date = start_date.split('T')[0]
    end_date = end_date.split('T')[0]

    start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = dt.datetime.strptime(end_date, '%Y-%m-%d')

    # Format the start_date and end_date to be compatible with SQL
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')

    # SQL query to retrieve data for the specific company within the selected date range

    # SQL query to retrieve data for the specific company within the selected date range

    fig = go.Figure()

    # Load data directly into a Pandas DataFrame using the SQL query

    if graph_type == 'candlestick':
        query = f"""
        SELECT date, open, high, low, close
        FROM daystocks
        WHERE cid = {company_id} AND date >= '{start_date}' AND date <= '{end_date}'
        ORDER BY date
        """
        df = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date'])
        fig.add_trace(go.Candlestick(x=df.index,
                                         open=df['open'],
                                         high=df['high'],
                                         low=df['low'],
                                         close=df['close'],
                                         name=f'Company {company_id}'))
    else:
        query = f"""
        SELECT date, value
        FROM stocks
        WHERE cid = {company_id} AND date >= '{start_date}' AND date <= '{end_date}'
        ORDER BY date
        """
        df = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date'])
        
        fig.add_trace(go.Scatter(x=df.index, y=df['value'], mode='lines', name=f'Company {company_id}'))

    fig.update_layout(title='Stock Prices',
                      xaxis_title='Date',
                      yaxis_title='Stock Price',
                      xaxis_rangeslider_visible=False)


    return fig
    

if __name__ == '__main__':
    app.run(debug=True)
