import os
import time
from google.cloud import pubsub_v1
import json
from google.cloud import bigquery


def insert_to_table(rowdata):
    """Handle Messages:Extract data. If column not present insert NULL.Using SQL query"""

    client = bigquery.Client()
    table_name=rowdata["table"]
    timestamp=rowdata["timestamp"]
    data=rowdata["data"]
    colnames=["A","B","C","D","E"]
    outdic={}
    for key in colnames:
        if key not in data.keys():
            outdic[key]='NULL'
        else:
            outdic[key]=data[key]

    query = ('INSERT INTO infusionsoft_dataset.{} \
		(Timestamp,A,B,C,D,E) VALUES \
		(Timestamp(\'{}\'),{},{},{},{},{})'.\
		format(table_name,timestamp,outdic["A"],outdic["B"],\
		outdic["C"],outdic["D"],outdic["E"]))

    print ("SQL QUERY:\n",query)
    query_job = client.query( query,location='US')



def receive_messages(project, subscription_name):
    """Receives messages from a pull subscription."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project, subscription_name)

    def callback(message):
        loaded_data = json.loads(message.data.decode('utf-8'))

        insert_to_table(loaded_data)
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    # The subscriber is non-blocking, so we must keep the main thread from
    # exiting to allow it to process messages in the background.
    print('Listening for messages on {}'.format(subscription_path))
    while True:
        time.sleep(60)

receive_messages(os.environ['PROJECT_ID'], os.environ['MY_SUBSCRIPTION'])
