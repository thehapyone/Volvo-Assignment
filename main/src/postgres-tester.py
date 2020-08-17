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


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')




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



data_info = ['e07fee3dad-b31c-43c2-948d-ddf8a939270a', '2020-02-27T10:53:37.662430415Z', 'v1.3.6', 'ICL', 'uswest2', 'a4f6fb2cb43c3fd2d84bc531f3c35822', 'PD', '685eb9db-594f-11ea-9b4f-caff7e0b273b', 'service_scheduled']


data_content = ['e07fee3dad-b31c-43c2-948d-ddf8a939270a', '685eb9db-594f-11ea-9b4f-caff7e0b273b', '026a9f35-d2b1-4381-af88-91c5e65f61d6', '15c3d5c9-fe60-4273-acc4-d7063215b64a', 'TOKSXCKACVMSRAA64RSZYQ', 'AIVD', 'DeliverToCar', 'Scheduled', '2020-02-27T10:53:35Z', '2020-02-28T03:00:00Z', 'NULL', 'NULL']


data_dev =['e07fee3dad-b31c-43c2-948d-ddf8a939270a', 'icl-service-app', 'v2.52.0', 'v2.52.0-1-g641c4fd', 'production', 'system', 'partner', '/partner/api/v1/service-schedule', 'POST']


try:

    connect_str = "dbname='testpython' user='ayo' host='localhost' " + \
                  "password='12345' port='5432'"
    # use our connection values to establish a connection
    conn = psycopg2.connect(connect_str)
    # create a psycopg2 cursor that can execute queries
    cursor = conn.cursor()

    table_commands = create_tables()
    # create the tables
    """
    for command in table_commands:
        cursor.execute(command)
    """
    # create a new table with a single column called "name"

    print('1')
    print(len(data_info))
    print(len(data_content))
    print(len(data_dev))

    insert_service_info( cursor, data_info)
    print('1')
    insert_service_content( cursor, data_content)
    print('1')
    insert_service_dev(cursor, data_dev)
    print('1')

    # run a SELECT statement - no data in there, but we can try it
    cursor.execute("""SELECT * from Service_Info""")
    conn.commit() # <--- makes sure the change is shown in the database
    rows = cursor.fetchall()
    print(rows)
    cursor.close()
    conn.close()
except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)
