import dash
from dash import dcc, html, dash_table
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime as dt
import numpy as np

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
companies_options = [{'label': row['name'] + " - " + row['symbol'], 'value': row['id'] } for index, row in companies.iterrows()]

app.layout = html.Div([
    html.Div(className="app-container", children=[
        html.Div(className="dashboard-header", children="Market Dashboard"),
        
        html.Div(className="component", children=[
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
            html.Div(id='debug-output'),
        ]),
        
        html.Div(className="component", children=[
            dcc.Dropdown(id='company-dropdown',
                multi=True,
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
            dcc.Checklist(
                id='bollinger-bands-checkbox',
                options=[
                    {'label': 'Bollinger bands', 'value': 'show_bollinger'}
                ],
                value=[],
                labelStyle={'display': 'block'}
            ),
            dcc.DatePickerRange(
                id='date-picker-range',
                min_date_allowed=dt.datetime(2019, 1, 1),
                max_date_allowed=dt.datetime.now(),
                initial_visible_month=dt.datetime.now(),
                start_date=dt.datetime(2019, 1, 1),
                end_date=dt.datetime.now()
            ),
            dcc.Graph(id='graph'),
            html.Div(id='data-table')
        ])
    ])
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
    [ddep.Output('data-table', 'children')],
    [ddep.Input('company-dropdown', 'value')]
)
def update_data(company_ids):
    if not company_ids:
        return [html.Div("No companies selected")]

    company_ids_str = ', '.join(map(str, company_ids))

    query = f"""
    SELECT date, low, high, open, close, volume, cid
    FROM daystocks
    WHERE cid IN ({company_ids_str})
    ORDER BY cid, date DESC
    """

    table = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date'])
    table['Company'] = table['cid'].map(companies.set_index('id')['name'])
    table = table[['Company', 'low', 'high', 'open', 'close', 'volume']]
    
    data_table = dash_table.DataTable(
        columns=[{'name': i, 'id': i} for i in table.columns],
        data=table.to_dict('records'),
        style_table={'overflowX': 'auto'}
    )

    return [data_table]




@app.callback(
    [ddep.Output('graph', 'figure')],
    [ddep.Input('company-dropdown', 'value'),
     ddep.Input('date-picker-range', 'start_date'),
     ddep.Input('date-picker-range', 'end_date'),
     ddep.Input('graph-type', 'value'),
     ddep.Input('bollinger-bands-checkbox', 'value')]
)
def update_graph(company_id, start_date, end_date, graph_type='line', bollinger_bands=None):
    if company_id is None:
        return [go.Figure()]


    start_date = start_date.split('T')[0]
    end_date = end_date.split('T')[0]

    start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = dt.datetime.strptime(end_date, '%Y-%m-%d')

    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.02)


    #DAYSTOCKS PART
    if graph_type == 'candlestick':
        for company in company_id:
            query = f"""
            SELECT date, open, high, low, close
            FROM daystocks
            WHERE cid = {company} AND date >= '{start_date}' AND date <= '{end_date}'
            ORDER BY date
            """
            df_stock = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date'])
            company_name = companies.loc[companies['id'] == company, 'name'].iloc[0]
            fig.add_trace(go.Candlestick(x=df_stock.index,
                                               open=df_stock['open'],
                                               high=df_stock['high'],
                                               low=df_stock['low'],
                                               close=df_stock['close'],
                                               name=f'{company_name}',
                                               increasing_line_color='green',
                                               decreasing_line_color='red',
                                               whiskerwidth=0.2, opacity=0.8,
                                               showlegend=True,
                                               hoverinfo='all'),
                                               row=1, col=1)
            

            fig.add_trace(go.Bar(x=df_stock.index, y=df_stock['volume'], name=f'{company_name}'), row=2, col=1)
            fig.update_yaxes(type='log', row=2, col=1)
            
            if 'show_bollinger' in bollinger_bands:
                df_stock['20_MA'] = df_stock['close'].rolling(window=20).mean()
                df_stock['20_std'] = df_stock['close'].rolling(window=20).std()
                
                fig.add_trace(go.Scatter(x=df_stock.index,
                                         y=df_stock['20_MA'],
                                         mode='lines',
                                         name='20-day Moving Average'),
                                         row=1, col=1)
                
                fig.add_trace(go.Scatter(x=df_stock.index,
                                         y=df_stock['20_MA'] + 2 * df_stock['20_std'],
                                         mode='lines',
                                         line=dict(color='green', width=1),
                                         name='Upper Bollinger Band'),
                                         row=1,
                                         col=1)
                
                fig.add_trace(go.Scatter(x=df_stock.index,
                                         y=df_stock['20_MA'] - 2 * df_stock['20_std'],
                                         mode='lines', line=dict(color='green', width=1),
                                         name='Lower Bollinger Band'),
                                         row=1,
                                         col=1)
                
                # Remplir la zone entre les bandes de Bollinger, en arrière-plan
                fig.add_vrect(x0=df_stock.index[0], x1=df_stock.index[-1],
                                    fillcolor='rgba(0,100,80,0.2)', opacity=0.2,
                                    line=dict(width=0),
                                    layer='below',
                                    row="all", col=1)

            fig.update_layout(title=f'Candlestick Chart for Company {company_id}',
                                    xaxis_title='Date',
                                    yaxis_title='Price',
                                    xaxis_rangeslider_visible=True,
                                    xaxis=dict(type="category"))

            

    else:
        #STOCKS PART
        for company in company_id:
            query = f"""
            SELECT date, value, volume
            FROM stocks
            WHERE cid = {company} AND date >= '{start_date}' AND date <= '{end_date}'
            ORDER BY date
            """

            df_stock = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date'])

            avg = df_stock['value'].mean()
            company_name = companies.loc[companies['id'] == company, 'name'].iloc[0]
    
            fig.add_trace(go.Scatter(x=df_stock.index, y=df_stock['value'], mode='lines', name=f'{company_name}'), row=1, col=1)

            fig.add_trace(go.Bar(x=df_stock.index, y=df_stock['volume'], name=f'{company_name}'), row=2, col=1)
            fig.update_layout(title='Volume Comparison',
                                    xaxis_title='Date',
                                    yaxis_title='Volume',
                                    xaxis_rangeslider_visible=False)
            fig.update_yaxes(type='log', row=2, col=1)

            # Calculate moving average and Bollinger Bands if needed
            if 'show_bollinger' in bollinger_bands:
                df_stock['20_MA'] = df_stock['value'].rolling(window=20).mean()
                df_stock['20_std'] = df_stock['value'].rolling(window=20).std()

                fig.add_trace(go.Scatter(x=df_stock.index,
                                         y=df_stock['20_MA'],
                                         mode='lines',
                                         name=f'{company_name} - Company {company} - 20-day Moving Average'),
                                    row=1, col=1)
                
                fig.add_trace(go.Scatter(x=df_stock.index,
                                         y=df_stock['20_MA'] + 2 * df_stock['20_std'],
                                         mode='lines',
                                         line=dict(color='green', width=1),
                                         name=f'{company_name} - Company {company} - Upper Bollinger Band'),
                                    row=1, col=1)
                
                fig.add_trace(go.Scatter(x=df_stock.index,
                                         y=df_stock['20_MA'] - 2 * df_stock['20_std'],
                                         mode='lines', line=dict(color='green', width=1),
                                         name=f'{company_name} - Company {company} - Lower Bollinger Band'),
                                    row=1, col=1)

        # Add average line to the plot
        fig.add_hline(y=avg, line_dash="dot", line_color="red", annotation_text=f'Overall Average: {avg:.2f}',
                            annotation_position="bottom right", row=1, col=1)

        # Update layout
        fig.update_layout(title='Stock Prices Comparison',
                                xaxis_title='Date',
                                yaxis_title='Stock Price',
                                xaxis_rangeslider_visible=False)
    
    return [fig]



if __name__ == '__main__':
    app.run(debug=True)
