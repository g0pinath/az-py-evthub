
from __future__ import unicode_literals
from azure.eventhub import EventHubConsumerClient
from uamqp import BatchMessage, Message, types, constants  # type: ignore
from uamqp.message import MessageHeader  # type: ignore
from azure.core.settings import settings # type: ignore
#from azure.eventhub.error import EventDataError
import pandas as pd
import json
import os
import sys
import random
import string


import datetime
import calendar
import json
import logging
import six
encoding='UTF-8'
# foreach($j in $json.records){$j.properties.log | ConvertFrom-Json | where {$_.verb -eq "delete"}}
#$json =$str | ConvertFrom-Json -Depth 10
#usage -- must pass a path for example on windows py .\receive-newer-messages-only.py c:\temp\
def get_messages() :
    connection_str = os.environ['EVENT_HUB_CONN_STRING']
    consumer_group = "$Default"
    eventhub_name =  'insights-logs-kube-audit-admin' #'storage-create-delete'  
    client = EventHubConsumerClient.from_connection_string(connection_str, consumer_group, eventhub_name=eventhub_name)    
    final_df = pd.DataFrame()
    def on_event(partition_context, event):
        print("Received event from partition {}".format(partition_context.partition_id))
        jsonbody = event.body_as_json(encoding='UTF-8')
        print("----------------------------------------")
        print(jsonbody)
        print("----------------------------------------")

        letters = string.ascii_lowercase
        filename=''.join(random.choice(letters) for i in range(10))+".json"
        fileFullPath=sys.argv[1]+filename
        print(fileFullPath)
            
            #print(event.message)
        with open(fileFullPath, 'w+') as f:
            print( jsonbody, file=f)
        with open(fileFullPath, 'r+') as f:
            print(f.read())
           
    with client:
        client.receive(
            on_event=on_event, 
            #starting_position="-1",  # "-1" is from the beginning of the partition. 
            #Max_wait_time - no activitiy for that much - call back function is called with No events.
        )
    return final_df

df = get_messages()
df.head()