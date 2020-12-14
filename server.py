import boto3
import psycopg2
from configparser import ConfigParser

queueName = 'lgq1003-1'

def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def connectDB():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(port=5433,**params)
		
        # create a cursor
        global cur
        cur = conn.cursor()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            print('Database connected.')
            return conn

def connectSQS():
    sqs = boto3.resource('sqs')
    try:
        queue = sqs.get_queue_by_name(QueueName=queueName)
    except:
        queue = sqs.create_queue(QueueName=queueName, Attributes={'DelaySeconds': '5'})
        print("Queue doesn't exist. Automatically create one")
    #queue.purge()
    for message in queue.receive_messages(MessageAttributeNames=['Widget', 'FieldOperatorAdd', 'FieldOperatorRemove']):
        if 'Widget' in message.message_attributes:
            response = message.body.split(".")
            processWidgetRequest(response[0], response[1], response[2], conn)
            message.delete()
        
        if 'FieldOperatorAdd' in message.message_attributes:
            response = message.body.split(".")
            print(response)
            processOperatorAddRequest(response[0], response[1], response[2], conn)
            message.delete()

        if 'FieldOperatorRemove' in message.message_attributes:
            response = message.body.split(".")
            print(response)
            processOperatorRemoveRequest(response[0], response[1], response[2], conn)
            message.delete()

def connectSNS():
    sns = boto3.client('sns')
    response = sns.create_topic(Name='my-topic')
    response = sns.publish(
        PhoneNumber='6782372859',
        Message='Hello World!',    
    )

def connectSES():
    ses = boto3.client('ses')

    response = ses.verify_email_identity(
        EmailAddress = 'lgq1003@wildcats.unh.edu'
    )
    print(response)

def processWidgetRequest(widget, city, county_name,conn):
    if county_name:
        cur.execute('''SELECT city, state_name, county_name, lat, lng, ST_Distance(ST_Transform(pos,3857), ST_Transform(uscities.the_geom,3857)) AS distance
                        FROM (SELECT the_geom AS pos FROM uscities WHERE uscities.city = '{0}' and uscities.county_name = '{1}') AS p, uscities
                        WHERE uscities.is_added='true'
                        ORDER BY distance ASC
                        LIMIT 5;'''.format(city,county_name))
        count = 1
        locations=[]
        records = cur.fetchall()
        print("Closest field operators to " + str(widget) + " :")
        print("__________________________")
        for row in records:
            print("(" + str(count) + ")")
            print("City: " + str(row[0]))
            print("State: " + str(row[1]))
            print("County: " + str(row[2]))
            print("Latitude: "+ str(row[3]))
            print("Longitude: "+ str(row[4]))
            print("Distance: "+ str(float(row[5])/(1000*1.6)) + " mile(s)")
            print(" ")
            count+=1
    else: print("No field operator was found")


def processOperatorAddRequest(fieldOperator, city, county_name, conn):
    if county_name:
        cur.execute('''UPDATE uscities SET is_added = 'true' WHERE city='{0}' AND county_name='{1}';'''.format(city,county_name))
        print("Field operator in {0}-{1} is added!".format(city,county_name))
    else: print("No such location was found")

def processOperatorRemoveRequest(fieldOperator, city, county_name,conn):
    if county_name:
        cur.execute('''UPDATE uscities SET is_added = 'false' WHERE city='{0}' AND county_name='{1}';'''.format(city,county_name))
        print("Field operator in {0}-{1} is removed!".format(city,county_name))
    else: print("No field operator was found")

if __name__ == '__main__':
    #conn = connectDB()
    #queue = connectSQS()
    connectSES()
