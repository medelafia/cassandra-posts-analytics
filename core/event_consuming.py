from kafka.consumer import KafkaConsumer 
import json
from core.db_utils import insert_event

TOPIC="likes-topic"

consumer = KafkaConsumer(
    TOPIC , 
     bootstrap_servers=['localhost:9092'],
    group_id='my_consumer_group',     # Manages offsets for the group
    auto_offset_reset='earliest',     # Start from beginning if no offset exists
    enable_auto_commit=True,          # Automatically mark messages as read
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def start_consuming() : 
    print("Consuming thread started...")
    try :
        for data in consumer : 
            event = data.value
            insert_event({'user_id' : event['user_id'] , 'post_id' : event['post_id']})
    except InterruptedError : 
        print("Stop consuming")
    finally : 
        consumer.close() 