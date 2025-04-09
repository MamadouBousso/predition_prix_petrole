from polygon import RESTClient
from quixstreams import Application
import json
import time
from datetime import datetime, timedelta
import os
from typing import Optional, Dict, List

class PolygonCollector:
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the Polygon.io collector
        
        Args:
            api_key: Polygon.io API key. If not provided, will look for POLYGON_API_KEY environment variable
        """
        self.api_key = api_key or os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("API key must be provided either directly or via POLYGON_API_KEY environment variable")
        
        self.client = RESTClient(self.api_key)
        
        # Oil and gas related tickers
        self.symbols = [
            'CL.COMM',  # Crude Oil
            'NG.COMM',  # Natural Gas
            'XOM',      # ExxonMobil
            'CVX',      # Chevron
            'BP',       # BP
            'SHEL',     # Shell
            'RB.COMM',  # RBOB Gasoline
            'HO.COMM'   # Heating Oil
        ]
        
        # Initialize Kafka connection
        self.app = Application(
            broker_address='127.0.0.1:31234',
            consumer_group='polygon-collector',
            auto_offset_reset='earliest'
        )
        
        # Create topic for oil and gas data
        self.topic = self.app.topic(
            name='polygon-oil-gas-stream',
            value_serializer='json'
        )

    def get_real_time_data(self) -> Dict:
        """
        Collect real-time data for all symbols
        
        Returns:
            Dict containing the latest data for each symbol
        """
        data = {}
        for symbol in self.symbols:
            try:
                # Get last trade
                trades = self.client.get_last_trade(symbol)
                
                # Get daily aggregates (OHLCV)
                today = datetime.now().strftime('%Y-%m-%d')
                aggs = self.client.get_daily_open_close(symbol, today)
                
                # Get previous day's close for calculating change
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                prev_aggs = self.client.get_daily_open_close(symbol, yesterday)
                
                data[symbol] = {
                    'timestamp': datetime.now().isoformat(),
                    'symbol': symbol,
                    'last_price': trades.price if trades else None,
                    'last_size': trades.size if trades else None,
                    'open': aggs.open if aggs else None,
                    'high': aggs.high if aggs else None,
                    'low': aggs.low if aggs else None,
                    'close': aggs.close if aggs else None,
                    'volume': aggs.volume if aggs else None,
                    'previous_close': prev_aggs.close if prev_aggs else None,
                    'change_percent': ((aggs.close - prev_aggs.close) / prev_aggs.close * 100) 
                                    if (aggs and prev_aggs and prev_aggs.close) else None
                }
                
            except Exception as e:
                print(f"Error collecting data for {symbol}: {str(e)}")
                continue
        
        return data

    def get_historical_data(self, 
                          from_date: str, 
                          to_date: str, 
                          multiplier: int = 1, 
                          timespan: str = 'day') -> Dict[str, List]:
        """
        Get historical data for all symbols
        
        Args:
            from_date: Start date in format YYYY-MM-DD
            to_date: End date in format YYYY-MM-DD
            multiplier: The size of the timespan multiplier
            timespan: The timespan to aggregate by. Options: minute, hour, day, week, month, quarter, year
            
        Returns:
            Dict containing historical data for each symbol
        """
        historical_data = {}
        
        for symbol in self.symbols:
            try:
                # Get aggregates for the date range
                aggs = self.client.get_aggs(
                    symbol,
                    multiplier,
                    timespan,
                    from_date,
                    to_date,
                    limit=50000
                )
                
                historical_data[symbol] = [{
                    'timestamp': agg.timestamp,
                    'open': agg.open,
                    'high': agg.high,
                    'low': agg.low,
                    'close': agg.close,
                    'volume': agg.volume,
                    'vwap': agg.vwap,
                    'transactions': agg.transactions
                } for agg in aggs]
                
            except Exception as e:
                print(f"Error collecting historical data for {symbol}: {str(e)}")
                continue
        
        return historical_data

    def stream_to_kafka(self, interval_seconds: int = 60):
        """
        Stream real-time data to Kafka at specified intervals
        
        Args:
            interval_seconds: Number of seconds to wait between data collections
        """
        print(f"Starting to stream data for symbols: {', '.join(self.symbols)}")
        
        with self.app.get_producer() as producer:
            while True:
                try:
                    # Collect data
                    data = self.get_real_time_data()
                    
                    # Send to Kafka
                    message = self.topic.serialize(
                        key=str(int(time.time())),
                        value=data
                    )
                    producer.produce(
                        topic=self.topic.name,
                        value=message.value,
                        key=message.key
                    )
                    
                    print(f"Data sent to Kafka at {datetime.now().isoformat()}")
                    
                except Exception as e:
                    print(f"Error in stream_to_kafka: {str(e)}")
                
                time.sleep(interval_seconds)

def main():
    # You need to set your Polygon.io API key in the environment
    # export POLYGON_API_KEY='your_api_key_here'
    
    try:
        collector = PolygonCollector()
        
        # Example: Get some historical data
        historical_data = collector.get_historical_data(
            from_date='2024-01-01',
            to_date='2024-04-09',
            multiplier=1,
            timespan='day'
        )
        print("Historical data collected successfully")
        
        # Start streaming real-time data
        collector.stream_to_kafka(interval_seconds=60)
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
