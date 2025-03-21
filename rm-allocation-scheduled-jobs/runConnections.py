import json
from os import getenv
import pandas as pd
import boto3
import requests
import configsettings

class RunConnections:
    def __init__(self, logger, params, overrides, context):
        self.logger = logger
        self.rm_rd_conn = params['rdconn']
        self.rm_wr_conn = params['wrconn']
        self.sqs = params['sqs']
        self.cache_client = params['cache_client']

    def run(self):
        query = "SELECT run_id,dtd_start,dtd_end,is_connections_required FROM allocation_run_audit WHERE is_connections_started IS NULL and is_run_completed='1' and is_connections_required='1'"
        rows = pd.read_sql(query,  self.rm_rd_conn)
        for index,row in rows.iterrows():
            run_id = row["run_id"] 
            dtd_start = row["dtd_start"] 
            dtd_end = row["dtd_end"] 
            self.runConnections( run_id, dtd_start, dtd_end)

   
    def runConnections(self, run_id, dtd_start, dtd_end):
        self.logger.info("Inside method")
        # All flights are done, push run_id to the connection_queue
        Payload =  {
                                "run_id": run_id, 
                                "start_interval": dtd_start, 
                                "end_interval": dtd_end, 
                                "run_type": "Scheduled_Connections", 
                                "market_list_table_name": "market_list_connections",
                                "route_type": "connections",
                                "update_navitaire_method": "sftp"
                            }


                    
        self.make_lambda_api_call(Payload,run_id)
        # function_name = configsettings.FETCH_INPUTS_LAMBDA  # Replace with your Lambda function's name

        # # Invoke the Lambda function
        # client = boto3.client('lambda')
        # response = client.invoke(
        #     FunctionName = function_name,
        #     InvocationType = 'RequestResponse',
        #     Payload = json.dumps({
        #                 "body": json.dumps({
        #                         "run_id": run_id, 
        #                         "start_interval": dtd_start, 
        #                         "end_interval": dtd_end, 
        #                         "run_type": "Scheduled_Connections", 
        #                         "market_list_table_name": "market_list_connections",
        #                         "route_type": "connections",
        #                         "update_navitaire_method": "sftp"
        #                     })


        #             })
        #     )

        # self.logger.info(response)

        # if response['StatusCode'] == 200:
        #     self.updateConnectionsStarted(run_id)
        #     result = json.loads(response['Payload'].read())
        #     # Handle the result here
        #     print(result)
        # else:
        #     # Handle errors
        #     print("Error invoking Lambda function")
            
            
    def updateConnectionsStarted(self,runid):
        query=f"update allocation_run_audit set is_connections_started='1' where run_id='{runid}'"
        self.rm_wr_conn.execute(query)
        
    def make_lambda_api_call(self,request,run_id):
        url = configsettings.FETCH_INPUTS_API_URL
        headers = {
            'Content-Type': 'application/json',
            'x-api-key': configsettings.RM_GW_API_KEY
        }
        if getenv('env', default=None)=="prod":
            headers.pop('x-api-key')
            response = requests.post(url, headers=headers, json=request)
            self.updateConnectionsStarted(run_id)
            self.logger.info("SUCCESS")
        else:
            response = requests.post(url, headers=headers, json=request)
            self.updateConnectionsStarted(run_id)
            self.logger.info("SUCCESS")
        