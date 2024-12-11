import json
from uuid import uuid4
from datetime import datetime
from sqlalchemy import create_engine
from ownlogger import OwnLogger
from updateRbd import UpdateRBDClass


def lambda_handler(event, context):
        records = event['Records']
        for record in records:
            content = record['body']

            allocationStartTime = datetime.now()

            allocation_data = json.loads(content)
            run_id = allocation_data["runId"]
            au_classes = allocation_data['auClasses']
            start_date = allocation_data['startDate']
            end_date = allocation_data['endDate']
            flight = allocation_data['flight']

            identifier = flight.split(" ")[1][2:]        
            carrier_code = flight.split(" ")[1][:2]
            origin = flight.split(" ")[2][:3]
            destination = flight.split(" ")[2][3:]
            logger = OwnLogger(run_id)
            logger.info(allocation_data)
            logger.info(type(content))
            urc = UpdateRBDClass(run_id)
            start_date_f = urc.format_date(start_date)
            end_date_f = urc.format_date(end_date)
            urc.log_time.update_run_id(run_id)
            countOfFailures = 0
            countOfTotal = 0
            inventory_dict = {}
            try:
                # First fetch the legKey for the market using tripInfo API
                leg_keys = urc.get_leg_keys(start_date_f, end_date_f,
                                             origin, destination, identifier, carrier_code)

                # Fetch dcs inventory for each leg key
                inventory_dict = urc.get_dcs_inventories(leg_keys)
                
            except Exception as e:
                logger.error(e)
                countOfFailures = len(au_classes)
                countOfTotal = len(au_classes)

            # Now we have all the data needed to update the AUs. For fetched inventory classes, set the allotted units
            allocation_result = urc.update_au_in_navitaire(inventory_dict, au_classes, run_id)

            for _, v in allocation_result.items():
                if v == -3:
                    countOfFailures += 1
                countOfTotal += 1
            
            allocationEndTime = datetime.now()
            urc.insertNavAudit(str(uuid4()), allocationStartTime, allocationEndTime,
                                content, json.dumps(allocation_result), run_id, allocation_data["b2cRunId"], 
                                allocation_data["b2bRunId"], countOfFailures, countOfTotal)
            urc.updateRunAudit(run_id, start_date_f, identifier, json.dumps(allocation_result))
            urc.updateAllocationAudit(allocationEndTime, run_id)
            urc.log_time.send_time_log_db()

        return {
            'statusCode': 200,
            'body': 'ok'
        }

# messageBody = {'runId': '80ee0d91-3493-4907-be1d-6181742daee7', 'type': 'L', 'flight': ' QP1410 BOMDEL', 'startDate': '12-11-2024', 'endDate': '12-11-2024', 'dayOfWeek': 'Daily', 'aircraftType': '', 'aircraftSuffix': '', 'lid': '', 'capacity': '', 'status': '', 'b2cRunId': 'e4bfe37b-440c-4ccf-95ea-b88112c6e84b', 'b2bRunId': '9460355b-799f-4a8b-97b9-ce661aacc580', 'rowId': '6d988a82-03b9-45f4-b994-6b503b9cc176', 'auClasses': {}}
# event = {'Records': [{'body': json.dumps(messageBody)}]}
# lambda_handler(event, None)
