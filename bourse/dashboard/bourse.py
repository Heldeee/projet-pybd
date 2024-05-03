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

# Global variable to store selected markets
selected_markets = []

# Function to filter companies based on selected markets
def filter_companies(selected_markets):
    if not selected_markets:
        return companies_options
    else:
        filtered_companies = companies[companies['mid'].isin(selected_markets)]
        filtered_options = [{'label': row['name'] + " - " + row['symbol'], 'value': row['id'] } for index, row in filtered_companies.iterrows()]
        return filtered_options

companies_options = [{'label': row['name'] + " - " + row['symbol'], 'value': row['id'] } for index, row in companies.iterrows()]

all_markets = pd.read_sql_query("SELECT id, name FROM markets", engine)

pastel_colors = [
    '204, 204, 255',  # Light blue
    '255, 204, 204',  # Light pink
    '204, 255, 204',  # Light green
    '255, 255, 204',  # Light yellow
    '255, 204, 255',  # Light purple
    '204, 255, 255',  # Light cyan
    '255, 230, 204',  # Light orange
    '230, 255, 204',  # Light lime
    '204, 230, 255',  # Light sky blue
    '255, 204, 230'   # Light rose
]


app.layout = html.Div([
    html.Link(
        rel='stylesheet',
        href='/assets/styles.css'
    ),
    html.Div(className="app-container", children=[
        dcc.Markdown('''
        # Markets Dashboard
        ### Python Big Data Project for EPITA
                     
        This dashboard allows you to visualize stock prices for different companies.
        You can select one or more companies from the dropdown, and the dashboard will display the stock prices.
        '''),
        
        # Autres composants sur la même ligne
        html.Div(className="component", children=[
            dcc.Dropdown(id='company-dropdown',
                multi=True,
                options=companies_options,
                placeholder='Select one or more companies',
            ),
        ]),
        dcc.Markdown('''#### Filter by Markets'''),
        html.Div(className="component", children=[
            dcc.Checklist(
                id='markets-filters',
                className='check-box',
                options=[{'label': row['name'], 'value': row['id'] } for index, row in all_markets.iterrows()],
                value='',
                labelStyle={'display': 'inline-block'}
            ),
        ]),
        # Graph et tableau de données
        html.Div(className="component",  children=[
            dcc.Graph(id='graph')
        ]),
        
        html.Div(className="component", children=[
            html.Div(className="grid-container", children=[
            dcc.DatePickerRange(
                id='date-picker-range',
                min_date_allowed=dt.datetime(2019, 1, 1),
                max_date_allowed=dt.datetime.now(),
                initial_visible_month=dt.datetime.now(),
                start_date=dt.datetime(2019, 1, 1),
                end_date=dt.datetime.now(),
                className='DateInput_input',
            ),
            dcc.RadioItems(
                id='graph-type',
                className='check-box',
                options=[
                    {'label': 'Line', 'value': 'line'},
                    {'label': 'Candlestick', 'value': 'candlestick'}
                ],
                value='line',
                labelStyle={'display': 'inline-block'}
            ),
            dcc.Checklist(
                id='bollinger-bands-checkbox',
                className='check-box',
                options=[
                    {'label': 'Bollinger bands', 'value': 'show_bollinger'}
                ],
                value=[],
                labelStyle={'display': 'block'}
            ),
            dcc.Checklist(
                id='avg-checkbox',
                className='check-box',
                options=[
                    {'label': 'Average', 'value': 'show_avg'}
                ],
                value=[],
                labelStyle={'display': 'block'}
            ),
            dcc.Checklist(
                id='log-scale-checkbox',
                className='check-box',
                options=[
                    {'label': 'Log scale', 'value': 'log_scale'}
                ],
                value=[],
                labelStyle={'display': 'block'}
            ),
            ]),
        ]),
        
        html.Div(className="dash-table", children=[
            dcc.Markdown('''
                         ## Data Table
                         Selected companies appears in tabs below. Click on a tab to see the data table.
                            '''),
            dcc.Tabs(id='tabs', value=[], children=[]),
            html.Div(id='tabs-content'),
        ]),

        # Bouton Export CSV
        html.Div(className="export-button", children=[
            html.Button('Export CSV', id='export-button', n_clicks=0)
        ]),
        
        # Composante Download pour le téléchargement du CSV
        dcc.Download(id='download-csv'),
        dcc.Markdown('''
                     2024 - leo.devin - phu-hung.dang - alexandre1.huynh
                        '''),
    ])
])

# Update selected markets global variable
@app.callback(
    ddep.Output('markets-filters', 'value'),
    [ddep.Input('markets-filters', 'value')]
)
def update_selected_markets(selected_markets_value):
    global selected_markets
    selected_markets = selected_markets_value
    return selected_markets_value

# Update company dropdown options based on selected markets
@app.callback(
    ddep.Output('company-dropdown', 'options'),
    [ddep.Input('markets-filters', 'value')]
)
def update_company_options(selected_markets):
    return filter_companies(selected_markets)



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
        page_size=10,
        style_cell={
            'textAlign': 'right',
            'padding': '5px',
            'color': 'white',
            'border': '1px solid rgb(48, 48, 48)',
            'backgroundColor': '#303030'
        },
        style_data={
            'color': 'white',
            'backgroundColor': 'rgb(119, 118, 123)'
        },
        style_data_conditional=[
        {
            'if': {'row_index': 'odd'},
            'backgroundColor': 'rgb(48, 48, 48)',
            'color': 'white'
        }
        ],
        style_header={
            'backgroundColor': 'rgb(48, 48, 48)',
            'color': 'white',
            'fontWeight': 'bold'
        }
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
        company_symbol = companies.loc[companies['id'] == int(company_id), 'symbol'].iloc[0]
        df_stats = get_dataframe_for_tab(company_id)
        
        csv_string = df_stats.to_csv(index=False, encoding='utf-8-sig')
        
        return dict(content=csv_string, filename=f"{company_symbol}_data.csv")

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

gray_color = "#b2b2b2"

@app.callback(
    [ddep.Output('graph', 'figure')],
    [ddep.Input('company-dropdown', 'value'),
     ddep.Input('date-picker-range', 'start_date'),
     ddep.Input('date-picker-range', 'end_date'),
     ddep.Input('graph-type', 'value'),
     ddep.Input('bollinger-bands-checkbox', 'value'),
     ddep.Input('avg-checkbox', 'value'),
     ddep.Input('log-scale-checkbox', 'value')]  # New input for log scale option
)
def update_graph(company_id, start_date, end_date, graph_type='line', bollinger_bands=None, avg_option=False, log_scale=False):  # Updated function signature
    if company_id is None:
        fig = go.Figure(layout=go.Layout(
            plot_bgcolor='#303030',
            paper_bgcolor='#303030',
            font=dict(color=f'{gray_color}'),
            legend=dict(font=dict(color=f'{gray_color}')),
        ))
        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor=f'{gray_color}')
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor=f'{gray_color}')
        return [fig]

    fig = go.Figure(
        layout=go.Layout(
            plot_bgcolor='#303030',
            paper_bgcolor='#303030',
            font=dict(color=f'{gray_color}'),
            legend=dict(font=dict(color=f'{gray_color}')),
        )
    )
    
    if graph_type == 'candlestick':
        for id, company in enumerate(company_id):
            query = f"""
            SELECT date, open, high, low, close, volume
            FROM daystocks
            WHERE cid = {company} AND date >= '{start_date}' AND date <= '{end_date}'
            ORDER BY date
            """
            df_stock = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date'])
            company_name = companies.loc[companies['id'] == company, 'name'].iloc[0]
            company_symbol = companies.loc[companies['id'] == company, 'symbol'].iloc[0]
            color = pastel_colors[id % len(pastel_colors)]
            avg = df_stock['close'].mean()
            if avg_option:
                fig.add_trace(go.Scatter(x=df_stock.index,
                                     y=np.full(df_stock.shape[0], avg),
                                     mode='lines',
                                     name=f'{company_name} - Average',
                                     line=dict(color='orange', width=1, dash='dash')))
            text = [f"Date: {date}<br>Open: {open}<br>High: {high}<br>Low: {low}<br>Close: {close}<br>Volume: {volume}" for date, open, high, low, close, volume in zip(df_stock.index, df_stock['open'], df_stock['high'], df_stock['low'], df_stock['close'], df_stock['volume'])]
            fig.add_trace(go.Candlestick(x=df_stock.index,
                                        open=df_stock['open'],
                                        high=df_stock['high'],
                                        low=df_stock['low'],
                                        close=df_stock['close'],
                                        name=f'{company_name}',
                                        increasing_line_color=f'rgb({color})',
                                        decreasing_line_color='firebrick',
                                        whiskerwidth=0.2,
                                        opacity=0.8,
                                        text=text,
                                        hoverinfo="text"))

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
                                         line=dict(color=f'rgb({color})', width=1),
                                         name='Upper Bollinger Band'))

                fig.add_trace(go.Scatter(x=df_stock.index,
                                         y=df_stock['20_MA'] - 2 * df_stock['20_std'],
                                         mode='lines',
                                         line=dict(color=f'rgb({color})', width=1),
                                         name='Lower Bollinger Band',
                                         fill='tonexty',
                                         fillcolor=f'rgba({color},0.2)'))


            fig.update_layout(xaxis_rangeslider_visible=False)

    else:
        for id, company in enumerate(company_id):
            query = f"""
            SELECT date, value
            FROM stocks
            WHERE cid = {company} AND date >= '{start_date}' AND date <= '{end_date}'
            ORDER BY date
            """

            df_stock = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date'])
            company_name = companies.loc[companies['id'] == company, 'name'].iloc[0]
            company_symbol = companies.loc[companies['id'] == company, 'symbol'].iloc[0]
            avg = df_stock['value'].mean()
            color = pastel_colors[id % len(pastel_colors)]
            if avg_option:
                fig.add_trace(go.Scatter(x=df_stock.index,
                                     y=np.full(df_stock.shape[0], avg),
                                     mode='lines',
                                     name=f'{company_name} - Average',
                                     line=dict(color='orange', width=1, dash='dash')))
            fig.add_trace(go.Scatter(x=df_stock.index,
                                 y=df_stock['value'],
                                 mode='lines',
                                 line=dict(color=f'rgb({color})', width=1),
                                 name=f'{company_name} - {company_symbol}',
                                 hovertemplate='<b>Date</b>: %{x|%Y-%m-%d}<br>' +
                                                  '<b>Price</b>: %{y:.2f}<extra></extra><br>' +
                                                  f'<b>Company</b>: {company_name} - {company_symbol}'
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
                                         line=dict(color=f'rgb({color})', width=1),
                                         name=f'{company_name} - Upper Bollinger Band'))
                fig.add_trace(go.Scatter(x=df_stock.index,
                                         y=df_stock['20_MA'] - 2 * df_stock['20_std'],
                                         mode='lines',
                                         line=dict(color=f'rgb({color})', width=1),
                                         name=f'{company_name} - Lower Bollinger Band',
                                         fill='tonexty',
                                         fillcolor=f'rgba({color}, 0.2)'))


    if log_scale:  # Apply log scale if selected
        fig.update_yaxes(type='log')

    fig.update_layout(title='Stock Prices Comparison',
                    yaxis_title='Stock Price',
                    xaxis_rangeslider_visible=False,
                    grid=dict(rows=1, columns=1, pattern='independent'),
                    font=dict(color='white')
    )

    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor=f'{gray_color}')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor=f'{gray_color}')

    return [fig]



if __name__ == '__main__':
    app.run(debug=True)
