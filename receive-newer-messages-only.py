
from azure.eventhub import EventHubConsumerClient
import pandas as pd
import json
import os
import sys
import random
import string

#usage -- must pass a path for example on windows py .\receive-newer-messages-only.py c:\temp\
def get_messages() :
    connection_str = os.environ['EVENT_HUB_CONN_STRING']
    consumer_group = "$Default"
    eventhub_name =  'insights-logs-kube-audit-admin' #'storage-create-delete'  
    client = EventHubConsumerClient.from_connection_string(connection_str, consumer_group, eventhub_name=eventhub_name)    
    final_df = pd.DataFrame()
    def on_event(partition_context, event):
        print("Received event from partition {}".format(partition_context.partition_id))
        body=event.body
        print(body)
        print("........")
        #event_df  = pd.DataFrame(body)
        #generate random file name
        letters = string.ascii_lowercase
        filename=''.join(random.choice(letters) for i in range(10))+".txt"
        fileFullPath=sys.argv[1]+filename
        message_body = list(body)
        
        #print(message_body)
        message_content = json.loads(body)
        print(message_content)
        print("-=-=-=-=-=-=-=-=-=-=")

        #nonlocal final_df
        #final_df = pd.concat([final_df,event_df],ignore_index= True)
        partition_context.update_checkpoint()
        with open(fileFullPath, 'w+') as f:
            print( message_content, file=f)
        with open(fileFullPath, 'r+') as f:
            print(f.read())

        print("---------------------")
    with client:
        client.receive(
            on_event=on_event, 
            #starting_position="-1",  # "-1" is from the beginning of the partition. 
            #Max_wait_time - no activitiy for that much - call back function is called with No events.
        )
    return final_df

df = get_messages()
df.head()