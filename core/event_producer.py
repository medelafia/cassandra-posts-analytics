from kafka.producer import KafkaProducer 
import json 
from core.db_utils import get_posts_uuids, get_users_uuids
import time 
import random 


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'] , 
    value_serializer =lambda v : json.dumps(v).encode("UTF-8") 
)

TOPIC="likes-topic"
user_uuids = get_users_uuids()
post_uuids = get_posts_uuids()


def start_producing() :
    print("Producing thread started...")
    try :
        for i in range(1000):
            data = { 
                    'user_id' : str(user_uuids[random.randint(0 , len(user_uuids) - 1 )]) , 
                    'post_id' : str(post_uuids[random.randint(0 , len(post_uuids) - 1 )])
                    }
            print("Producing : " , data)
            producer.send(TOPIC , value=data)
            time.sleep(0.5)
    except InterruptedError : 
        print("Stoppig producing")
    finally : 
        producer.close()