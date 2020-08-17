"""
##########################################################################
############# Solution 2 - Implementation Schema for Schema ################################################
############################################################################
Implementation Script for transforming the JSON service events to the suggested schema

@:author: Ayo Ayibiowu
@contact: ayo.ayibiowu@outlook.com

"""

import json
import os
from time import sleep

# gets the current working director
working_dir = os.path.dirname(__file__)

# using the sample.json file
json_file_path = os.path.join(working_dir, "service_subset.json")

# json data
json_data = dict()

# Field Names for the Service_Info table
headers_service_info = ['id', 'time', 'version', 'product', 'backendRegion', 'xRequestId', 'privacyClass', 'flowId',
                        'contentCategory']
# Field Names for the Service_Dev table
headers_service_dev = ['id', 'application', 'applicationVersion', 'buildVersion', 'environment', 'origin', 'channel',
                       'path', 'method']
# Field Names for the Service_Content table
headers_service_content = ['id', 'requestId', 'serviceId', 'subId', 'vin', 'serviceProviderName', 'serviceMainType',
                           'serviceStatus',
                           'startTime', 'endTime', 'startLocation', 'endLocation']

content_location_info = ['startLocation', 'endLocation']
headers_location = ["longitude", "latitude"]
# service content key
content_name = "content"


def extract_table_service(data):
    """
    Extract out the values for the cols in the suppose Service_Info table.
    Also add the requestId in the Content into the table as well
    :param data: Dictionary of values
    :return: list of values
    """
    return [data[col] for col in headers_service_info]


def extract_table_service_dev(data):
    """
    Extract out the values for the cols in the suppose Service_Dev table.
    Also add the requestId in the Content into the table as well
    :param data: Dictionary of values
    :return: list of values
    """
    return [data[col] for col in headers_service_dev]


def parse_content_table(data_in):
    """
    Parse the content JSON data into a structured table for the Service_Content table
    :param data: Dictionary of values with the service content
    :return: list of values
    """
    # fetch the contents data out
    data = data_in['content']
    data_keys = [my_keys for my_keys in data.keys()]
    result = [data[col] if col not in content_location_info else parse_location(data, col) for col in
              headers_service_content if
              col in data_keys]
    # filter content without location information and add None for those content
    # check the data_keys for location info
    for loc in content_location_info:
        if loc not in data_keys:
            result.append('NULL')

    ## lastly, insert the id into the data as well
    result.insert(0, data_in['id'])
    return result


def parse_location(data, col):
    """
    Helps in parsing location data containing both longitude and latitude
    :param data:
    :return: a serialized location data. For example: -122.410591,37.641449
    """
    temp = [data[col][loc_col] for loc_col in headers_location]
    return ','.join(map(str, temp))


if __name__ == '__main__':
    print("Starting up: Task 2 ---- Schema Transformation")
    sleep(1)

    try:
        # now attempt to open the JSON file
        with open(json_file_path) as service_json:
            json_data = json.load(service_json)

        print(type(json_data))
        # print(json_data['content'])

        # extract the values for the Service_Info
        table_service_info = [extract_table_service(data) for data in json_data]
        # extract the values for the Service_Dev
        table_service_dev = [extract_table_service_dev(data) for data in json_data]
        # extract out the values for the SERVICE_CONTENT Table
        table_service_content = [parse_content_table(data) for data in json_data]

        print(len(table_service_content))
        print(len(table_service_info))
        print(len(table_service_dev))

        print(table_service_content)
        #


    except KeyboardInterrupt:
        print("Keyboard Interrupt ")

    except Exception as e:
        print("Exception occurred - ", e)

print("Done here")
