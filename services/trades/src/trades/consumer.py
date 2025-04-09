# Create a consumer to verify the message
from quixstreams import Application

app = Application(
    broker_address='127.0.0.1:31234',
    consumer_group='example-consumer',
    auto_offset_reset='earliest',
    consumer_config={
        'enable.auto.commit': False  # Setting this explicitly as a boolean
    }
)

# Define the same topic
topic = app.topic(name='my_topic', value_serializer='json')

# Create a consumer and read messages
with app.get_consumer([topic]) as consumer:
    for message in consumer.stream():
        print(f"Received message: {message.value}")
