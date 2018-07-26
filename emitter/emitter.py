import os
from google.cloud import pubsub_v1
import json
import time
from random import randint,shuffle

def get_table():
    """select a table with table name 'table_x' where x=[1,2..100]"""
   
    return 'table_'+str(randint(0,100))


def add_vals_to_cols(colnames):
    """Adds dummy data to each of selected columns and returns dictionary"""
    
    out_dic={}
    for elem in colnames:
        #selecting dummy data from 0-100
        out_dic[elem]=randint(0,100)
    return out_dic

def get_random_colnames(colnames):
    """Algorithm for random column selection
    1)shuffle the colnames 
    2)choose a random index from 0 to len(colnames)-1 i
    3)return list of selected colnames in random manner"""
    
    shuffle(colnames)
    return sorted(colnames[0:randint(0, len(colnames))])

def create_and_emit_messages(project, topic_name):
    """Create JSON according to specs and Emits messages to a Pub/Sub topic"""
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic_name)

    #We specify the five colnames
    colnames=["A","B","C","D","E"]

    #Select a random list of colums of any length say. A,C,D
    selected_cols = get_random_colnames(colnames)
    #print (selected_cols)

    #Attaching a dummy data for each of selected column
    data = add_vals_to_cols(selected_cols)
    #print (data)

    #Attaching a random table from ['table_1'-'table_100']
    table = get_table()
    #print (table)

    #Get the current timestamp
    timestamp=time.strftime('%Y-%m-%d %H:%M:%S')
    #print (timestamp)

    full_data={"data": data,"table":table,"timestamp":timestamp} 
    json_string = json.dumps(full_data)
    print ('JSON:\n',json_string)
    
    #publish the data
    data_encoded = json_string.encode('utf-8')
    publisher.publish(topic_path, data=data_encoded)

create_and_emit_messages(os.environ['PROJECT_ID'], os.environ['MY_TOPIC'])
