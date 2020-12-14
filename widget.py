import boto3

global id
id = 100;
queueName = 'lgq1003-1'

def connectSQS():
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queueName)
    return queue

def sendRequest(queue):
    global id
    widget = input("Widget: ")
    city = input("City: ")
    county_name = input("County: ")
    message = widget + "." + city + "." + county_name
    response = queue.send_messages(Entries=[
        {
            'Id': str(id),
            'MessageBody': message,
            'MessageAttributes': {
                'Widget': {
                    'StringValue': widget,
                    'DataType': 'String'
                }
            }
        }
    ])
    id+=1;

if __name__ == '__main__':
    queue = connectSQS()
    while (1):
        sendRequest(queue)
    
