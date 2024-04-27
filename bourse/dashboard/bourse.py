import dash
from dash import dcc, html, dash_table
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
import plotly.graph_objects as go
from dateutil.relativedelta import relativedelta
import datetime as dt
import numpy as np

import logging  

LOG = logging.getLogger(__name__)

DATABASE_URI = 'timescaledb://ricou:monmdp@db:5432/bourse'    # inside docker
# DATABASE_URI = 'timescaledb://ricou:monmdp@localhost:5432/bourse'  # outisde docker
engine = sqlalchemy.create_engine(DATABASE_URI)

app = dash.Dash(__name__,  title="Bourse", suppress_callback_exceptions=True)
server = app.server

# Search bar with smart search (dropdown)
companies = pd.read_sql_query("SELECT * FROM companies", engine)
companies_options = [{'label': row['name'] + " - " + row['symbol'], 'value': row['id'] } for index, row in companies.iterrows()]

app.layout = html.Div([
    html.Link(
        rel='stylesheet',
        href='/assets/styles.css'
    ),
    html.Div(className="app-container", children=[
        html.Div(className="dashboard-header", children="Market Dashboard"),
        
        # Premier composant repliable avec la requête SQL
        html.Div(className="component", children=[
            html.Details(open=False, children=[
                html.Summary("SQL Query"),
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

                html.Div(id='export-csv')
            ])
        ]),
        
        # Autres composants sur la même ligne
        html.Div(className="component", style={"flex": "1", "margin-left": "20px"}, children=[
            dcc.Dropdown(id='company-dropdown',
                multi=True,
                options=companies_options,
                placeholder='Select one or more companies'
            ),
        ]),
        
        html.Div(className="component", style={"flex": "1", "margin-left": "20px"}, children=[
            dcc.DatePickerRange(
                id='date-picker-range',
                min_date_allowed=dt.datetime(2019, 1, 1),
                max_date_allowed=dt.datetime.now(),
                initial_visible_month=dt.datetime.now(),
                start_date=dt.datetime(2019, 1, 1),
                end_date=dt.datetime.now()
            ),
        ]),
        
        html.Div(className="component", style={"flex": "1", "margin-left": "20px"}, children=[
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
            dcc.Checklist(
                id='avg-checkbox',
                options=[
                    {'label': 'Average', 'value': 'show_avg'}
                ],
                value=[],
                labelStyle={'display': 'block'}
            ),
            dcc.Checklist(
                id='log-scale-checkbox',
                options=[
                    {'label': 'Log scale', 'value': 'log_scale'}
                ],
                value=[],
                labelStyle={'display': 'block'}
            ),
        ]),
        
        html.Div(className="component", style={"flex": "1", "margin-left": "20px"}, children=[
            dcc.Tabs(id='date-range-button', value=[], children=[
                dcc.Tab(label='1D', value='1D'),
                dcc.Tab(label='5D', value='5D'),
                dcc.Tab(label='1M', value='1M'),
                dcc.Tab(label='3M', value='3M'),
                dcc.Tab(label='1Y', value='1Y'),
                dcc.Tab(label='2Y', value='2Y'),
                dcc.Tab(label='5Y', value='5Y')
            ]),
        ]),
        # Graph et tableau de données
        html.Div(className="component", style={"flex": "1", "margin-top": "20px"}, children=[
            dcc.Graph(id='graph'),
            
            html.Div(className="dashboard-header", children="Data Table"),
            dcc.Tabs(id='tabs', value=[], children=[]),
            html.Div(id='tabs-content'),
        ]),

        # Bouton Export CSV
        html.Button('Export CSV', id='export-button', n_clicks=0),
        
        # Composante Download pour le téléchargement du CSV
        dcc.Download(id='download-csv')
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
    ddep.Output('tabs', 'children'),
    [ddep.Input('company-dropdown', 'value')]
)
def update_tabs(company_ids):
    if not company_ids:
        return []
    
    tabs = []
    for company_id in company_ids:
        company_name = companies.loc[companies['id'] == company_id, 'name'].iloc[0]
        company_symbol = companies.loc[companies['id'] == company_id, 'symbol'].iloc[0]
        tabs.append(dcc.Tab(label=f"{company_name} - {company_symbol}", value=f'tab-{company_id}'))

    return tabs


@app.callback(
    ddep.Output('tabs-content', 'children'),
    [ddep.Input('tabs', 'value')]
)
def update_tab_content(selected_tab):
    if not selected_tab:
        return html.Div()

    company_id = selected_tab.split('-')[-1]
    query = f"""
    SELECT date, low, high, open, close, volume
    FROM daystocks
    WHERE cid = '{company_id}'
    ORDER BY date ASC
    """

    # Fetch data from the database into a Pandas DataFrame
    df = pd.read_sql_query(query, engine, parse_dates=['date'])

    # Calculate additional statistics
    df_stats = df.groupby(df['date'].dt.date).agg({
        'low': 'min',
        'high': 'max',
        'open': 'first',
        'close': 'last',
        'volume': 'sum'
    })
    df_stats['average'] = df.groupby(df['date'].dt.date)['close'].mean().round(2)
    df_stats['std_dev'] = df.groupby(df['date'].dt.date)['close'].std().round(2)

    df_stats['Date'] = df_stats.index.map(lambda x: x.strftime('%d/%m/%Y'))

    df_stats = df_stats[['Date' ,'low', 'high', 'open', 'close', 'volume', 'average', 'std_dev']]

    # Rename columns
    df_stats = df_stats.rename(columns={
        'low': 'Min',
        'high': 'Max',
        'open': 'Début',
        'close': 'Fin',
        'volume': 'Volume',
        'average': 'Moyenne',
        'std_dev': 'Écart type'
    })

    #table columns with Date Excluded
    non_sortable = ['Date']

    table_css = [
    {
        'selector': f'th[data-dash-column="{col}"] span.column-header--sort',
        'rule': 'display: none',
    }
    for col in non_sortable
]

    table = dash_table.DataTable(
        id={'type': 'dynamic-table', 'index': company_id},
        columns=[{'name': i, 'id': i} for i in df_stats.columns],
        data=df_stats.reset_index().to_dict('records'),
        css=table_css,
        sort_action='native',
        sort_mode='single',
        style_table={'overflowX': 'auto'},
        style_data={'whiteSpace': 'normal', 'height': 'auto'},
    )

    # return table
    return html.Div([table], id=f'table-{company_id}')


@app.callback(
    ddep.Output('download-csv', 'data'),
    [ddep.Input('export-button', 'n_clicks')],
    [ddep.State('tabs', 'value')]
)
def export_csv(n_clicks, selected_tab):
    if n_clicks and selected_tab:
        # Obtenir le DataFrame correspondant à l'onglet sélectionné
        company_id = selected_tab.split('-')[-1]
        df_stats = get_dataframe_for_tab(company_id)
        
        csv_string = df_stats.to_csv(index=False, encoding='utf-8-sig')
        
        return dict(content=csv_string, filename=f"company_{company_id}_data.csv")

def get_dataframe_for_tab(company_id):
    query = f"""
    SELECT date, low, high, open, close, volume
    FROM daystocks
    WHERE cid = '{company_id}'
    ORDER BY date ASC
    """
    df = pd.read_sql_query(query, engine, parse_dates=['date'])
    df_stats = df.groupby(df['date'].dt.date).agg({
        'low': 'min',
        'high': 'max',
        'open': 'first',
        'close': 'last',
        'volume': 'sum'
    })
    df_stats['average'] = df.groupby(df['date'].dt.date)['close'].mean().round(2)
    df_stats['std_dev'] = df.groupby(df['date'].dt.date)['close'].std().round(2)
    df_stats['Date'] = df_stats.index.map(lambda x: x.strftime('%d/%m/%Y'))
    df_stats = df_stats[['Date' ,'low', 'high', 'open', 'close', 'volume', 'average', 'std_dev']]
    df_stats = df_stats.rename(columns={
        'low': 'Min',
        'high': 'Max',
        'open': 'Début',
        'close': 'Fin',
        'volume': 'Volume',
        'average': 'Moyenne',
        'std_dev': 'Écart type'
    })
    return df_stats


def define_date(start_date, end_date, range):
    now = dt.datetime.now()
    if start_date and end_date:
        return start_date, end_date
    else:
        match range:
            case '1D':
                start_date = now - relativedelta(days=1)
            case '5D':
                start_date = now - relativedelta(days=5)
            case '1M':
                start_date = now - relativedelta(months=1)
            case '3M':
                start_date = now - relativedelta(months=3)
            case '1Y':
                start_date = now - relativedelta(years=1)
            case '2Y':
                start_date = now - relativedelta(years=2)
            case '5Y':
                start_date = now - relativedelta(years=5)
        return start_date, now


@app.callback(
    [ddep.Output('graph', 'figure')],
    [ddep.Input('company-dropdown', 'value'),
     ddep.Input('date-picker-range', 'start_date'),
     ddep.Input('date-picker-range', 'end_date'),
     ddep.Input('graph-type', 'value'),
     ddep.Input('bollinger-bands-checkbox', 'value'),
     ddep.Input('date-range-button', 'value'),
     ddep.Input('avg-checkbox', 'value'),
     ddep.Input('log-scale-checkbox', 'value')]  # New input for log scale option
)
def update_graph(company_id, start_date, end_date, graph_type='line', bollinger_bands=None, date_range=None, avg_option=False, log_scale=False):  # Updated function signature
    if company_id is None:
        return [go.Figure()]

    start_date, end_date = define_date(start_date, end_date, date_range)

    fig = go.Figure()
    
    if graph_type == 'candlestick':
        for company in company_id:
            query = f"""
            SELECT date, open, high, low, close, volume
            FROM daystocks
            WHERE cid = {company} AND date >= '{start_date}' AND date <= '{end_date}'
            ORDER BY date
            """
            df_stock = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date'])
            company_name = companies.loc[companies['id'] == company, 'name'].iloc[0]
            avg = df_stock['close'].mean()
            if avg_option:
                fig.add_trace(go.Scatter(x=df_stock.index,
                                     y=np.full(df_stock.shape[0], avg),
                                     mode='lines',
                                     name=f'{company_name} - Average',
                                     line=dict(color='red', width=1, dash='dash')))
            fig.add_trace(go.Candlestick(x=df_stock.index,
                                        open=df_stock['open'],
                                        high=df_stock['high'],
                                        low=df_stock['low'],
                                        close=df_stock['close'],
                                        name=f'{company_name}',
                                        increasing_line_color='green',
                                        decreasing_line_color='red',
                                        whiskerwidth=0.2,
                                        opacity=0.8,
                                        hoverinfo='x+y+z',
                                        showlegend=True))

            if 'show_bollinger' in bollinger_bands:
                df_stock['20_MA'] = df_stock['close'].rolling(window=20).mean()
                df_stock['20_std'] = df_stock['close'].rolling(window=20).std()

                fig.add_trace(go.Scatter(x=df_stock.index,
                                         y=df_stock['20_MA'],
                                         mode='lines',
                                         name='20-day Moving Average'))

                fig.add_trace(go.Scatter(x=df_stock.index,
                                         y=df_stock['20_MA'] + 2 * df_stock['20_std'],
                                         mode='lines',
                                         line=dict(color='green', width=1),
                                         name='Upper Bollinger Band'))

                fig.add_trace(go.Scatter(x=df_stock.index,
                                         y=df_stock['20_MA'] - 2 * df_stock['20_std'],
                                         mode='lines',
                                         line=dict(color='green', width=1),
                                         name='Lower Bollinger Band'))

                fig.add_vrect(x0=df_stock.index[0], x1=df_stock.index[-1],
                              fillcolor='rgba(0,100,80,0.2)', opacity=0.2,
                              line=dict(width=0),
                              layer='below')

            fig.update_layout(title=f'Candlestick Chart for Company {company_id}',
                              xaxis_title='Date',
                              yaxis_title='Candlestick Price',
                              xaxis_rangeslider_visible=False)

    else:
        for company in company_id:
            query = f"""
            SELECT date, value
            FROM stocks
            WHERE cid = {company} AND date >= '{start_date}' AND date <= '{end_date}'
            ORDER BY date
            """

            df_stock = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date'])
            company_name = companies.loc[companies['id'] == company, 'name'].iloc[0]
            avg = df_stock['value'].mean()
            fig.add_trace(go.Scatter(x=df_stock.index,
                                 y=df_stock['value'],
                                 mode='lines',
                                 name=f'{company_name}',
                                 hovertemplate='<b>Date</b>: %{x|%Y-%m-%d}<br>' +
                                                  '<b>Price</b>: %{y:.2f}<extra></extra>'
                                 ))

            if 'show_bollinger' in bollinger_bands:
                df_stock['20_MA'] = df_stock['value'].rolling(window=20).mean()
                df_stock['20_std'] = df_stock['value'].rolling(window=20).std()

                fig.add_trace(go.Scatter(x=df_stock.index,
                                         y=df_stock['20_MA'],
                                         mode='lines',
                                         name=f'{company_name} - 20-day Moving Average'))

                fig.add_trace(go.Scatter(x=df_stock.index,
                                         y=df_stock['20_MA'] + 2 * df_stock['20_std'],
                                         mode='lines',
                                         line=dict(color='green', width=1),
                                         name=f'{company_name} - Upper Bollinger Band'))

                fig.add_trace(go.Scatter(x=df_stock.index,
                                         y=df_stock['20_MA'] - 2 * df_stock['20_std'],
                                         mode='lines',
                                         line=dict(color='green', width=1),
                                         name=f'{company_name} - Lower Bollinger Band'))
                
            if avg_option:
                fig.add_trace(go.Scatter(x=df_stock.index,
                                     y=np.full(df_stock.shape[0], avg),
                                     mode='lines',
                                     name=f'{company_name} - Average',
                                     line=dict(color='red', width=1, dash='dash')))


    if log_scale:  # Apply log scale if selected
        fig.update_yaxes(type='log')

    fig.update_layout(title='Stock Prices Comparison',
                    yaxis_title='Stock Price',
                    xaxis_rangeslider_visible=False,
                    grid=dict(rows=1, columns=1, pattern='independent'))


    return [fig]



if __name__ == '__main__':
    app.run(debug=True)
