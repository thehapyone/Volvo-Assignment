import psycopg2

from configparser import ConfigParser


def config(filename='database.ini', section='postgresql'):
    # create a parser
    db_parser = ConfigParser()
    # read config file
    db_parser.read(filename)

    # get section, default to postgresql
    db_info = {}
    if db_parser.has_section(section):
        params = db_parser.items(section)
        for param in params:
            db_info[param[0]] = param[1]
    else:
        raise Exception("Config file error")

    return db_info

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE Service_Info (
            id VARCHAR(255) PRIMARY KEY,
            time VARCHAR(255) NOT NULL,
            version VARCHAR(255) NOT NULL,
            product VARCHAR(255) NOT NULL,
            backendRegion VARCHAR(255) NOT NULL,
            xRequestId VARCHAR(255) NOT NULL,
            privacyClass VARCHAR(255) NOT NULL,
            flowId VARCHAR(255) NOT NULL,
            contentCategory VARCHAR(255) NOT NULL
        )
        """,
        """ CREATE TABLE Service_Content (
                id VARCHAR(255) PRIMARY KEY,
                requestId VARCHAR(255) NOT NULL,
                serviceId VARCHAR(255) NOT NULL,
                subId VARCHAR(255) NOT NULL,
                vin VARCHAR(255) NOT NULL,
                serviceProviderName VARCHAR(255) NOT NULL,
                serviceMainType VARCHAR(255) NOT NULL,
                serviceStatus VARCHAR(255) NOT NULL,
                startTime VARCHAR(255) NOT NULL,
                endTime VARCHAR(255) NOT NULL , 
                startLocation VARCHAR(255) NOT NULL,
                endLocation VARCHAR(255) NOT NULL,
                FOREIGN KEY (id)
                REFERENCES Service_Info (id)
                ON UPDATE CASCADE ON DELETE CASCADE
                )
        """,
        """
        CREATE TABLE Service_Dev (
                id VARCHAR(255) PRIMARY KEY,
                application VARCHAR(255) NOT NULL,
                applicationVersion VARCHAR(255) NOT NULL,
                buildVersion VARCHAR(255) NOT NULL,
                environment VARCHAR(255) NOT NULL,
                origin VARCHAR(255) NOT NULL,
                channel VARCHAR(255) NOT NULL,
                path VARCHAR(255) NOT NULL,
                method VARCHAR(255) NOT NULL,
                FOREIGN KEY (id)
                REFERENCES Service_Info (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """)
    return commands


def insert_service_info(cur, data):
    """ insert a new record into the Service_info  """
    sql_info = """INSERT INTO Service_Info(id, time, version, product, backendRegion, xRequestId, privacyClass, flowId, contentCategory)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s );"""
    # execute the insert statement
    cur.execute(sql_info, tuple(data))

def insert_service_content( cur, data):
    """ insert a new record into the Service_Content """
    sql_content = """INSERT INTO Service_Content(id, requestId, serviceId, subId, vin, serviceProviderName, serviceMainType, serviceStatus, startTime, endTime, startLocation, endLocation)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );"""
    # execute the insert statement
    cur.execute(sql_content, tuple(data))

def insert_service_dev(cur, data):
    """ insert a new record into the Service_dev """
    sql_dev = """INSERT INTO Service_Dev(id, application, applicationVersion, buildVersion, environment, origin, channel, path, method)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    # execute the insert statement
    cur.execute(sql_dev, tuple(data))

