import dash
from dash import dcc
from dash import html
import plotly.graph_objs as go
from kafka import KafkaConsumer
import json
import threading
import queue
import pandas as pd
from collections import deque
import time

# Initialize the Dash application
app = dash.Dash(__name__)

# Queue to store the latest  data
data_queue = queue.Queue()

# Global variables to store the latest price and order book data received from Kafka
latest_price_data = {}
latest_order_book_data = {}

# Structures 
price_window = {symbol: deque(maxlen=10) for symbol in latest_price_data}
order_book_window = {symbol: deque(maxlen=10) for symbol in latest_order_book_data}

# Function to consume messages
def consume_kafka_data():
    consumer = KafkaConsumer(
        'stock-data',  # Kafka topic
        bootstrap_servers='localhost:9092',  # Kafka server address
        group_id='price_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        data = message.value
        symbol = data.get('symbol')

        # Update the latest price data for the symbol
        if 'price' in data:
            price_window[symbol].append(data['price'])  
            latest_price_data[symbol] = data['price']  

        # Update the order book data for the symbol
        if 'order_book' in data:
            order_book_window[symbol].append(data['order_book'])  
            latest_order_book_data[symbol] = data['order_book']  

        # Put the data into the queue for Dash to use
        data_queue.put((latest_price_data.copy(), latest_order_book_data.copy())) 

# Start a thread to consume Kafka data
def start_kafka_thread():
    kafka_thread = threading.Thread(target=consume_kafka_data)
    kafka_thread.daemon = True  

# Function to generate the price graph (average price over the last 10 minutes)
def generate_price_graph():
    symbols = list(latest_price_data.keys())
    avg_prices = [sum(price_window[symbol]) / len(price_window[symbol]) for symbol in symbols]  # Calculate average price

    return go.Figure(
        data=[go.Scatter(x=symbols, y=avg_prices, mode='lines+markers')],
        layout=go.Layout(
            title='Average Price of Symbols Over 10 Minutes',  # Updated title in English
            xaxis={'title': 'Symbols'},
            yaxis={'title': 'Average Price'}
        )
    )

# Function to generate the order book graph (max bid and ask)
def generate_order_book_graph():
    symbols = list(latest_order_book_data.keys())
    bids = [max([bid[0] for bid in order['bids']]) for order in latest_order_book_data.values()]
    asks = [max([ask[0] for ask in order['asks']]) for order in latest_order_book_data.values()]

    return go.Figure(
        data=[
            go.Bar(x=symbols, y=bids, name='Bids', marker={'color': 'green'}),
            go.Bar(x=symbols, y=asks, name='Asks', marker={'color': 'red'})
        ],
        layout=go.Layout(
            title='Order Book (Max Bid and Ask)',  
            xaxis={'title': 'Symbols'},
            yaxis={'title': 'Volume'}
        )
    )

# Configuration of the Dash layout
app.layout = html.Div([
    html.H1("Real-Time Dashboard"),  
    html.Div([
        dcc.Graph(id='price-graph'),
        dcc.Graph(id='order-book-graph'),
        dcc.Interval(
            id='interval-component',
            interval=60000,  
            n_intervals=0
        )
    ])
])

# Update the graphs
@app.callback(
    [dash.dependencies.Output('price-graph', 'figure'),
     dash.dependencies.Output('order-book-graph', 'figure')],
    [dash.dependencies.Input('interval-component', 'n_intervals')]
)
def update_graphs(n_intervals):
    # Check if new data is available
    if not data_queue.empty():
        latest_price_data, latest_order_book_data = data_queue.get()

    return generate_price_graph(), generate_order_book_graph()

# Start Kafka data consumption in a separate thread
start_kafka_thread()

# Run the Dash application
if __name__ == "__main__":
    app.run_server(debug=True)
