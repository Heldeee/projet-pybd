import dash
from dash import dcc, html, dash_table
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
import plotly.express as px
import plotly.graph_objects as go
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
                dcc.Graph(id='stock-graph',
                          config={'displayModeBar': False}),
                dcc.Graph(id='volume-graph'),
                dash_table.DataTable(id='raw-data-table')
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
    [ddep.Output('stock-graph', 'figure'),
     ddep.Output('volume-graph', 'figure'),
     ddep.Output('raw-data-table', 'data')],
    [ddep.Input('company-dropdown', 'value'),
     ddep.Input('date-picker-range', 'start_date'),
     ddep.Input('date-picker-range', 'end_date'),
     ddep.Input('graph-type', 'value'),
     ddep.Input('bollinger-bands-checkbox', 'value'),
     ddep.Input('stock-graph', 'clickData'),
     ddep.Input('volume-graph', 'clickData')]
)
def update_graph(company_id, start_date, end_date, graph_type='line', bollinger_bands=None,
                 stock_click_data=None, volume_click_data=None):
    if company_id is None:
        print("company is empty")
        return {}, {}, {}


    start_date = start_date.split('T')[0]
    end_date = end_date.split('T')[0]

    start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = dt.datetime.strptime(end_date, '%Y-%m-%d')

    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')

    fig_stock = go.Figure()
    fig_volume = go.Figure()

    if graph_type == 'candlestick':
        query = f"""
        SELECT date, open, high, low, close
        FROM daystocks
        WHERE cid = {company_id} AND date >= '{start_date}' AND date <= '{end_date}'
        ORDER BY date
        """
        df_stock = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date'])
        fig_stock = go.Figure(data=[go.Candlestick(x=df_stock.index,
                                                    open=df_stock['open'],
                                                    high=df_stock['high'],
                                                    low=df_stock['low'],
                                                    close=df_stock['close'],
                                                    name=f'Company {company_id}')])
        
        if 'show_bollinger' in bollinger_bands:
            df_stock['20_MA'] = df_stock['close'].rolling(window=20).mean()
            df_stock['20_std'] = df_stock['close'].rolling(window=20).std()
            fig_stock.add_trace(go.Scatter(x=df_stock.index, y=df_stock['20_MA'], mode='lines', name='20-day Moving Average'))
            fig_stock.add_trace(go.Scatter(x=df_stock.index, y=df_stock['20_MA'] + 2 * df_stock['20_std'], mode='lines', line=dict(color='green', width=1), name='Upper Bollinger Band'))
            fig_stock.add_trace(go.Scatter(x=df_stock.index, y=df_stock['20_MA'] - 2 * df_stock['20_std'], mode='lines', line=dict(color='green', width=1), name='Lower Bollinger Band'))
            
            # Remplir la zone entre les bandes de Bollinger, en arrière-plan
            fig_stock.add_vrect(x0=df_stock.index[0], x1=df_stock.index[-1],
                                fillcolor='rgba(0,100,80,0.2)', opacity=0.2,
                                line=dict(width=0),
                                layer='below',
                                row="all", col=1)

        fig_stock.update_layout(title=f'Candlestick Chart for Company {company_id}',
                                xaxis_title='Date',
                                yaxis_title='Price',
                                xaxis_rangeslider_visible=True)

    else:
        for company in company_id:
            query = f"""
            SELECT date, value
            FROM stocks
            WHERE cid = {company} AND date >= '{start_date}' AND date <= '{end_date}'
            ORDER BY date
            """
            df_stock = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date'])
            avg = df_stock['value'].mean()
            company_name = companies.loc[companies['id'] == company, 'name'].iloc[0]
    
            fig_stock.add_trace(go.Scatter(x=df_stock.index, y=df_stock['value'], mode='lines', name=f'{company_name}'))

            # Calculate moving average and Bollinger Bands if needed
            if 'show_bollinger' in bollinger_bands:
                df_stock['20_MA'] = df_stock['value'].rolling(window=20).mean()
                df_stock['20_std'] = df_stock['value'].rolling(window=20).std()
                fig_stock.add_trace(go.Scatter(x=df_stock.index, y=df_stock['20_MA'], mode='lines', name=f'{company_name} - Company {company} - 20-day Moving Average'))
                fig_stock.add_trace(go.Scatter(x=df_stock.index, y=df_stock['20_MA'] + 2 * df_stock['20_std'], mode='lines', line=dict(color='green', width=1), name=f'{company_name} - Company {company} - Upper Bollinger Band'))
                fig_stock.add_trace(go.Scatter(x=df_stock.index, y=df_stock['20_MA'] - 2 * df_stock['20_std'], mode='lines', line=dict(color='green', width=1), name=f'{company_name} - Company {company} - Lower Bollinger Band'))

        # Add average line to the plot
        fig_stock.add_hline(y=avg, line_dash="dot", line_color="red", annotation_text=f'Overall Average: {avg:.2f}',
                            annotation_position="bottom right")

        # Update layout
        fig_stock.update_layout(title='Stock Prices Comparison',
                                xaxis_title='Date',
                                yaxis_title='Stock Price',
                                xaxis_rangeslider_visible=False)

    for company in company_id:
        query_volume = f"""
        SELECT date, volume
        FROM stocks
        WHERE cid = {company} AND date >= '{start_date}' AND date <= '{end_date}'
        ORDER BY date
        """
        df_volume = pd.read_sql_query(query_volume, engine, index_col='date', parse_dates=['date'])

        fig_volume = px.bar(df_volume, x=df_volume.index, y='volume', labels={'x': 'Date', 'y': 'Volume'}, 
                            title=f'Volume of Stocks Traded for Company {company}')

        fig_volume.update_layout(title='Volume of Stocks Traded',
                                xaxis_title='Date',
                                yaxis_title='Volume',
                                xaxis_rangeslider_visible=False)

        # Si l'utilisateur a cliqué sur un point sur l'un des graphiques, mettre en évidence ce point sur l'autre graphique
        if stock_click_data:
            fig_volume.add_vline(x=stock_click_data['points'][0]['x'], line_dash="dash", line_color="red")
        elif volume_click_data:
            fig_stock.add_vline(x=volume_click_data['points'][0]['x'], line_dash="dash", line_color="red")

        # Afficher les données brutes dans un tableau
        stats_stock = df_stock.describe().transpose()
        stats_stock.columns = ['Count', 'Mean', 'Std', 'Min', '25%', '50%', '75%', 'Max']
        # stats_stock['Date'] = stats_stock.index
        # stats_stock['Date'] = stats_stock['Date'].dt.strftime('%Y-%m-%d')

        # Créer une liste de dictionnaires pour les données brutes
        table_data = stats_stock.to_dict('records')

    return fig_stock, fig_volume, table_data



if __name__ == '__main__':
    app.run(debug=True)
