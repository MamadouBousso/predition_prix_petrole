# Create an Application instance with Kafka configs
from quixstreams import Application


app = Application(
    broker_address='127.0.0.1:31234',  # Using IPv4 localhost address explicitly
    consumer_group='example',
    auto_offset_reset='earliest'
)

# Define a topic "my_topic" with JSON serialization
topic = app.topic(name='my_topic', value_serializer='json')

# Create a producer
with app.get_producer() as producer:
    while True:
        # creer un message
        event = {"id": "1", "text": "Lorem ipsum dolor sit amet"}

        # Serialize an event using the defined Topic 
        message = topic.serialize(key=event["id"], value=event)

        # Produce a message into the Kafka topic
        producer.produce(
        topic=topic.name, value=message.value, key=message.key)

        import time
        time.sleep(1)