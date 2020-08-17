"""
##########################################################################
############# Solution 3 - Kafka Service Events Consumer ##################
############################################################################
This script reads service events from the kafka topic "icl.analytics.events.service"
as soon as it is available and writes the data into Postgress.

Requirement:
 - A running kafka and Zookeeper server
 - an active kafka topic
 - Postgress

@:author: Ayo Ayibiowu
@contact: ayo.ayibiowu@outlook.com

"""

import json
import os
from time import sleep
from kafka import KafkaConsumer
# import the transformation script
from solution2 import *

# gets the current working director
working_dir = os.path.dirname(__file__)

# Kafka Server Address
kafka_server = ["localhost:9092"]
# kafka topic address
kafka_event_topic8 = "icl.analytics.events.service"
kafka_event_topic = ["python-trial2"]

# producer instance
service_consumer = None

if __name__ == '__main__':
    print("Starting up: Task 3 ---- Kafka Service Event Consumer")
    sleep(1)

    try:
        # create the Producer instance
        service_consumer = KafkaConsumer(bootstrap_servers=kafka_server, auto_offset_reset='earliest',
                                         enable_auto_commit=True, auto_commit_interval_ms=500, group_id='my-group',
                                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        # subscribe to topic
        service_consumer.subscribe(topics=kafka_event_topic)
        print("Consumer connected: ", service_consumer.bootstrap_connected())

    except KeyboardInterrupt:
        print("Keyboard Interrupt ")

    except Exception as e:
        print("Exception occurred - ", e)

    else:
        while True:
            try:
                # read the next message
                data = (next(service_consumer)).value
                # here check for data quality
                #### check data

                # extract the values for the Service_Info
                table_service_info = extract_table_service(data)
                # extract the values for the Service_Dev
                table_service_dev = extract_table_service_dev(data)
                # extract out the values for the SERVICE_CONTENT Table
                table_service_content = parse_content_table(data['content'])

                print(len(table_service_content))
                print(len(table_service_info))
                print(len(table_service_dev))

                print(table_service_info)
                print("-----------------------------------------")
                print(table_service_content)
                print("-----------------------------------------")
                print(table_service_dev)


            except KeyboardInterrupt:
                print("Keyboard Interrupt - Shutting down ")
                break

            except Exception as e:
                print("Exception occurred - ", e)
                break


print("Done here")
