"""
##########################################################################
############# Solution 4 - Kafka Service Events Producer##################
############################################################################
This script reads service events from JSON file and publishes the events data to the
kafka topic "icl.analytics.events.service" every 1 second.
Requirement:
 - A running kafka and Zookeeper server
 - service_subset.json file
 -
@:author: Ayo Ayibiowu
@contact: ayo.ayibiowu@outlook.com

"""

import json
import os
from time import sleep
from kafka import KafkaProducer

# gets the current working director
working_dir = os.path.dirname(__file__)

# using the sample.json file
json_file_path = os.path.join(working_dir, "service_subset.json")

# json data
json_data = None

# service content key
content_name = "content"

# Kafka Server Address
kafka_server = ["localhost:9092"]
# kafka topic address
kafka_event_topic8= "icl.analytics.events.service"
kafka_event_topic = "python-trial"

# producer instance
service_producer = None

def data_check(data):
    """
    Checks data for missing content and structure integrity
    :param data: A JSON object
    :return: A JSON Object and Quality_status
    """

if __name__ == '__main__':
    print("Starting up: Task 4 ---- Kafka Service Event Producer")
    sleep(1)

    try:
        # now attempt to open the JSON file
        with open(json_file_path) as service_json:
            json_data = json.load(service_json)

    except Exception as e:
        print("Unable to open JSON file ")
        print("Exception - ",e)

    else:
        print("JSON File loaded")
        try:
            # create the Producer instance
            service_producer = KafkaProducer(bootstrap_servers=kafka_server, value_serializer=lambda x: json.dumps(x).encode('utf-8'), retries=3)

            print ("Producer connected: ", service_producer.bootstrap_connected())
            for i in range (10):
                data, quality_status = data_check(json_data[i])
                if quality_status:
                    producer.send(kafka_event_topic, value=json_data[i])
                    producer.flush()
                    sleep(0.5)

            producer.close()

        except KeyboardInterrupt:
            print("Keyboard Interrupt ")

        except Exception as e:
            print("Exception occurred - ", e)

print("Done here")
