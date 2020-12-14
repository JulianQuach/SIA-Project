import boto3

global id
id = 900;
queueName = 'lgq1003-1'

def connectSQS():
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queueName)
    return queue

def sendAddRequest(queue):
    global id
    fieldOperator = input("Field Operator: ")
    city = input("City: ")
    county_name = input("County: ")
    message = fieldOperator + "." + city + "." + county_name
    response = queue.send_messages(Entries=[
        {
            'Id': str(id),
            'MessageBody': message,
            'MessageAttributes': {
                'FieldOperatorAdd': {
                    'StringValue': fieldOperator,
                    'DataType': 'String'
            }
        }
        }
    ])
    id+=1;

def sendRemoveRequest(queue):
    global id
    fieldOperator = input("Field Operator: ")
    city = input("City: ")
    county_name = input("County: ")
    message = fieldOperator + "." + city + "." + county_name
    response = queue.send_messages(Entries=[
        {
            'Id': str(id),
            'MessageBody': message,
            'MessageAttributes': {
                'FieldOperatorRemove': {
                    'StringValue': fieldOperator,
                    'DataType': 'String'
            }
        }
        }
    ])
    id+=1;

if __name__ == '__main__':
    queue = connectSQS()
    while (1):
        choice = input("Type 'Add' to add, 'Remove' to remove: ")
        if (choice.lower() == "add"):
            sendAddRequest(queue)
        elif (choice.lower() == "remove"):
            sendRemoveRequest(queue)
        else: print("Not recognized!")
    
