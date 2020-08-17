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
from database_helper import *
import psycopg2

# gets the current working director
working_dir = os.path.dirname(__file__)

# Kafka Server Address
kafka_server = ["localhost:9092"]
# kafka topic address
kafka_event_topic = "icl.analytics.events.service"

# producer instance
service_consumer = None

# database stuffs
cursor = None
conn = None

def create_service_tables(conn, cursor):
    """ Attempts to create table if not created before"""
    try:
        table_commands = create_tables()
        for command in table_commands:
            cursor.execute(command)
        print ("Tables created completed")

    except Exception as e:
        print ("Unable to create table - Probably created - ", e)
        conn.rollback()
        pass


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

        ## Attempting connection to the Postgres database
        # read connection parameters
        params = config()
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        # create a psycopg2 cursor that can execute queries
        cursor = conn.cursor()


    except KeyboardInterrupt:
        print("Keyboard Interrupt ")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    except Exception as e:
        print("Exception occurred - ", e)

    else:
        # create table for the first time
        create_service_tables(conn, cursor)

        while True:
            try:
                # read the next message
                data = (next(service_consumer)).value
                # here check for data quality
                #### check data
                # create a psycopg2 cursor that can execute queries
                cursor = conn.cursor()

                # extract the values for the Service_Info
                table_service_info = extract_table_service(data)
                # extract the values for the Service_Dev
                table_service_dev = extract_table_service_dev(data)
                # extract out the values for the SERVICE_CONTENT Table
                table_service_content = parse_content_table(data)
                print("New event - Transformation complete")

                """
                print(len(table_service_content))
                print(len(table_service_info))
                print(len(table_service_dev))

                print(table_service_info)
                print("-----------------------------------------")
                print(table_service_content)
                print("-----------------------------------------")
                print(table_service_dev)
                """
                print("Writing to DB")
                insert_service_info(cursor, table_service_info)
                insert_service_content(cursor, table_service_content)
                insert_service_dev(cursor, table_service_dev)
                # save the changes
                conn.commit()
                sleep(2)

            except KeyboardInterrupt:
                print("Keyboard Interrupt - Shutting down ")
                break

            except Exception as e:
                print("Exception occurred - ", e)
                break

    finally:
        # close connection to database if active
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
        # close connection to kafka
        if service_consumer is not None:
            service_consumer.close()

print("Done here")
