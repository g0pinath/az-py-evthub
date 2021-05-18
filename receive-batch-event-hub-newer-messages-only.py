
from azure.eventhub import EventHubConsumerClient
import pandas as pd
import json
import os
import sys
import random
import string

def get_messages() :    
    connection_str = os.environ['EVENT_HUB_CONN_STRING']
    consumer_group = "$Default"
    eventhub_name =  'insights-logs-kube-audit-admin' #'storage-create-delete'  
    client = EventHubConsumerClient.from_connection_string(connection_str, consumer_group, eventhub_name=eventhub_name)    
    final_df = pd.DataFrame()
    def on_event_batch(partition_context, events):
        print("Received event from partition {}".format(partition_context.partition_id))
        print(len(events))
        #Checking whether there is any event returned as we have set max_wait_time
        for event in events:
                #Event.body operation
            body=event.body
            event_df  = pd.DataFrame(body,index = [0])
                #generate random file name
            letters = string.ascii_lowercase
            filename=''.join(random.choice(letters) for i in range(10))+".json"
            fileFullPath=sys.argv[1]+filename
            print(fileFullPath)
            
            #print(event.message)
            with open(fileFullPath, 'w+') as f:
                print( event.message, file=f)
            with open(fileFullPath, 'r+') as f:
                print(f.read())
            nonlocal final_df
            final_df = pd.concat([final_df,event_df],ignore_index= True)
            partition_context.update_checkpoint()
            print("---------------------")
    with client:
        client.receive_batch(
            on_event_batch=on_event_batch, 
            #starting_position="-1",
            max_wait_time = 5,max_batch_size=2  # "-1" is from the beginning of the partition. 
            #Max_wait_time - no activitiy for that much - call back function is called with No events.
        )
    return final_df

df = get_messages()
df.head()