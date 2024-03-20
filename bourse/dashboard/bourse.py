import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy

# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

DATABASE_URI = 'timescaledb://ricou:monmdp@db:5432/bourse'    # inside docker
# DATABASE_URI = 'timescaledb://ricou:monmdp@localhost:5432/bourse'  # outisde docker
engine = sqlalchemy.create_engine(DATABASE_URI)

app = dash.Dash(__name__,  title="Bourse", suppress_callback_exceptions=True) # , external_stylesheets=external_stylesheets)
server = app.server
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
                html.Div(id='stock-market-visualizations')
             ])


@app.callback( ddep.Output('query-result', 'children'),
               ddep.Input('execute-query', 'n_clicks'),
               ddep.State('sql-query', 'value'),
             )
def run_query(n_clicks, query):
    if n_clicks > 0:
        try:
            result_df = pd.read_sql_query(query, engine)
            return html.Pre(result_df.to_string())
        except Exception as e:
            return html.Pre(str(e))
    return "Enter a query and press execute."


@app.callback(
    ddep.Output('stock-market-visualizations', 'children'),
    [ddep.Input('query-result', 'children')]  # You might use the query result to generate visualizations
)
def display_stock_market_visualizations(query_result):
    # Placeholder for stock market visualizations
    # You'll need to implement this part to display the requested features
    return html.Div([
        html.H3("Stock Market Visualizations"),
        # Add components for displaying stock market data as per your requirements
        # For example:
        # dcc.Graph(id='stock-price-graph'),
        # dcc.Graph(id='bollinger-bands-graph'),
        # html.Table(id='raw-data-table'),
        # Add more components as needed
    ])

if __name__ == '__main__':
    app.run(debug=True)
