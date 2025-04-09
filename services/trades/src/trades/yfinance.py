import yfinance as yf
from quixstreams import Application
import json
import time
from datetime import datetime

class YFinanceCollector:
    def __init__(self, symbols=None):
        # Default oil and gas symbols if none provided
        self.symbols = symbols or [
            'CL=F',    # Crude Oil Futures
            'BZ=F',    # Brent Oil Futures
            'NG=F',    # Natural Gas Futures
            'RB=F',    # Gasoline Futures
            'XOM',     # ExxonMobil
            'CVX',     # Chevron
            'BP',      # BP
            'SHEL'     # Shell
        ]
        
        # Initialize Kafka connection
        self.app = Application(
            broker_address='127.0.0.1:31234',
            consumer_group='yfinance-collector',
            auto_offset_reset='earliest'
        )
        
        # Create topic for oil and gas data
        self.topic = self.app.topic(
            name='oil-gas-stream',
            value_serializer='json'
        )

    def get_real_time_data(self):
        """Collect real-time data for all symbols"""
        data = {}
        for symbol in self.symbols:
            ticker = yf.Ticker(symbol)
            try:
                # Get current data
                info = ticker.info
                current_price = info.get('regularMarketPrice')
                previous_close = info.get('regularMarketPreviousClose')
                volume = info.get('regularMarketVolume')
                
                data[symbol] = {
                    'timestamp': datetime.now().isoformat(),
                    'symbol': symbol,
                    'current_price': current_price,
                    'previous_close': previous_close,
                    'volume': volume,
                    'change_percent': ((current_price - previous_close) / previous_close * 100) if previous_close else None
                }
            except Exception as e:
                print(f"Error collecting data for {symbol}: {str(e)}")
                continue
        
        return data

    def stream_to_kafka(self, interval_seconds=60):
        """Stream data to Kafka at specified intervals"""
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

if __name__ == "__main__":
    # Create collector instance
    collector = YFinanceCollector()
    
    # Start streaming with 1-minute intervals
    collector.stream_to_kafka(interval_seconds=60)
