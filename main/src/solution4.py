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
kafka_event_topic8 = "icl.analytics.events.service"
kafka_event_topic = "python-trial"

# producer instance
service_producer = None

# default_keys
default_keys_layout = ('id', 'time', 'version', 'product', 'application', 'applicationVersion', 'buildVersion',
                       'environment', 'backendRegion', 'origin', 'channel', 'path', 'method', 'userAgent', 'xRequestId',
                       'privacyClass', 'flowId', 'contentCategory', 'content')


def quality_checker(data):
    """
    Checks data for missing content and structure integrity
    :param data: A JSON object
    :return: A JSON Object and Quality_status
    """
    """
    - Ensure the expected keys are received in the data packet. No more or less. ::: Done
    - Alerts when field name are swapped leading to wrong ordering. ::: Done
    - Alerts when data of specified field is missing. Check if the content field values are missing ::: Done
    - Alerts when the id is not unique. The id is expected to be unique. ::: This can be done with Hash or by tracking previous id
    """
    # for tracking quality defects
    issue_counter = 0
    try:
        # Ensure expected keys are received in the data packet: No more or less
        current_keys = [ keys for keys in data ]
        if len(current_keys) != len(default_keys_layout):
            print("Ensure expected keys: Check Failed")
            issue_counter = issue_counter + 1

        # Alerts when field name are swapped leading to wrong ordering.
        # Will use Hash to achieve this. Compare the hash of the default key layout and the hash of the current layout.
        if hash(tuple(current_keys)) != hash(default_keys_layout):
            print("Field name are swapped:: Check failed")
            issue_counter = issue_counter + 1

        # Alerts when data of specified field is missing. Check if the content field values are missing
        content_values = [ data['content'][key] for key in data['content']]
        for value in content_values:
            if value in ["", "None"]:
                print("Content field values are missing:: Check failed")
                issue_counter = issue_counter + 1
                break

    except Exception as e:
        print("Exception in checker - ", e)
        issue_counter = issue_counter + 1
        pass
    finally:
        return data, issue_counter


if __name__ == '__main__':
    print("Starting up: Task 4 ---- Kafka Service Event Producer")
    sleep(1)

    try:
        # now attempt to open the JSON file
        with open(json_file_path) as service_json:
            json_data = json.load(service_json)

    except Exception as e:
        print("Unable to open JSON file ")
        print("Exception - ", e)

    else:
        print("JSON File loaded")
        try:
            # create the Producer instance
            service_producer = KafkaProducer(bootstrap_servers=kafka_server,
                                             value_serializer=lambda x: json.dumps(x).encode('utf-8'), retries=3)

            print("Producer connected: ", service_producer.bootstrap_connected())
            for i in range(100):
                data, quality_status = quality_checker(json_data[i])
                print ("sending ... ", 1)
                if quality_status == 0:
                    # means quality check passed. Send the data normally
                    service_producer.send(kafka_event_topic, value=data)
                    service_producer.flush()
                else:
                    # some or all quality check failed. Send a log/warning/error/repair channel
                    print("Quality check failed - ", quality_status)
                    print("Sending data to the repair channel....")
                sleep(0.5)
            service_producer.close()

        except KeyboardInterrupt:
            print("Keyboard Interrupt ")
            if service_producer is not None:
                service_producer.close()

        except Exception as e:
            print("Exception occurred - ", e)
            if service_producer is not None:
                service_producer.close()

print("Done here")
