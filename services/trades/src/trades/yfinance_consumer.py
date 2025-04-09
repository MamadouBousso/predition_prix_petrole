from quixstreams import Application
import json
from datetime import datetime

def main():
    # Initialize Kafka connection
    app = Application(
        broker_address='127.0.0.1:31234',
        consumer_group='yfinance-consumer',
        auto_offset_reset='earliest'
    )
    
    # Create topic reference
    topic = app.topic(
        name='oil-gas-stream',
        value_serializer='json'
    )
    
    print("Starting consumer for oil and gas data...")
    
    # Create consumer and start reading messages
    with app.get_consumer([topic]) as consumer:
        for message in consumer.stream():
            try:
                data = message.value
                print(f"\nReceived data at {datetime.now().isoformat()}:")
                for symbol, info in data.items():
                    print(f"{symbol}: ${info['current_price']:.2f} ({info['change_percent']:.2f}%)")
            except Exception as e:
                print(f"Error processing message: {str(e)}")

if __name__ == "__main__":
    main()
