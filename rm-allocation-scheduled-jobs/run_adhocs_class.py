import sys
import json
import warnings
from datetime import datetime
import croniter
from pyawscron import AWSCron
import pandas as pd
from collections import Counter
import configsettings

class SchedulerHandler:
    def __init__(self, logger, params, overrides, context):
        warnings.filterwarnings('ignore')
        self.USERNAME = "SYS2"

        self.logger = logger
        self.mongoconn = params['mongoconn']  # Single MongoDB connection
        self.sqs = params['sqs']
        self.events = params['events']
        self.cache_client = params['cache_client']
        self.constantutils = params['constantutils']
        
        # MongoDB database
        self.db = self.mongoconn['qp_dw_rmalloc']  # or self.mongoconn['your_db_name']

        self.concurrency = 300
        self.totalFlightDaysInRun = 2000
        self.totalTime = 120
        self.threshold_time = self.constantutils.THRESHOLD_TIME  # The minimum threshold time required to run a batch
        
        if 'threshold_time' in overrides:
            self.threshold_time = overrides['threshold_time']

        if 'concurrency' in overrides:
            self.concurrency = overrides['concurrency']

        if 'totalFlightDaysInRun' in overrides:
            self.totalFlightDaysInRun = overrides['totalFlightDaysInRun']

        if 'totalTime' in overrides:
            self.totalTime = overrides['totalTime']

    def run(self):
        self.logger.info("SUCCESS: Connection to MongoDB instance succeeded")
        print("SUCCESS: Connection to MongoDB instance succeeded")
        
        if self.cache_client.ping():
            self.logger.info("Connected to Redis")
            print("Connected to Redis")

        # main code
        scheduler_names = self.get_scheduler_names()
        dic = {}
        xz = self.get_shortest_run()

        for i in scheduler_names:
            temp_time = str(self.check_event_schedule(i))
            dic[temp_time] = i

        dic_keys = list(dic.keys())
        dic_keys.sort()

        # check size of queue if 0 then go ahead or skip
        response = self.sqs.get_queue_attributes(
            QueueUrl=configsettings.MARKET_LIST_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )
        approximate_number_of_messages = int(response['Attributes']['ApproximateNumberOfMessages'])
        approximate_number_of_messages_not_visible = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])
        
        if approximate_number_of_messages + approximate_number_of_messages_not_visible == 0:
            print("Scheduler Names:", dic)
            # Get current UTC time
            current_time_utc = datetime.now()
            print("Current time: ", current_time_utc)
            time_obj = datetime.strptime(dic_keys[0], '%Y-%m-%d %H:%M:%S')
            time_diff = time_obj - current_time_utc
            total_minutes = time_diff.total_seconds() // 60
            print("Time difference for", dic_keys[0], ":", total_minutes, "minutes")
            self.can_a_batch_run(total_minutes)

    def convert_quartz_to_cron(self, quartz_expression):
        parts = quartz_expression.split(" ")
        cron_expression = " ".join([parts[0]] + parts[1:4] + ["*"])
        return cron_expression

    def get_next_run_time(self, cron_expression):
        try:
            cron_expression = self.convert_quartz_to_cron(cron_expression)
            cron = croniter.croniter(cron_expression)
            next_run_time = cron.get_next(datetime)
            return next_run_time
        except Exception as e:
            print("Error:", e)
            return None

    def get_time(self, quartz_expression):
        try:
            aws_cron = AWSCron(quartz_expression)
            current_time = datetime.now()
            next_occurrence = aws_cron.occurrence(current_time).next()
            return next_occurrence.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            return None

    def extract_cron_expression(self, string):
        cron_expression = string.replace("cron", "").replace("(", "").replace(")", "").strip()
        return cron_expression

    def get_scheduler_names(self):
        """
        Get scheduler names from MongoDB
        """
        scheduler_names = []
        try:
            collection = self.db['config_schedulers']
            cursor = collection.find(
                {'allocationType': {'$ne': 'S'}},
                {'schedulerName': 1, '_id': 0}
            )
            scheduler_names = [doc['schedulerName'] for doc in cursor]
            print(scheduler_names)
        except Exception as e:
            self.logger.error(e)
            sys.exit()

        return scheduler_names

    def update_run_with_batches(self, run_id):
        """
        Increment batch_completed counter for a run
        """
        try:
            collection = self.db['allocation_run_audit']
            collection.update_one(
                {'run_id': run_id},
                {'$inc': {'batch_completed': 1}}
            )
        except Exception as e:
            print(e)

    def update_run_end(self, run_id):
        """
        Update run with end_time and increment batch_completed
        """
        end_time = datetime.utcnow()
        try:
            collection = self.db['allocation_run_audit']
            collection.update_one(
                {'run_id': run_id},
                {
                    '$inc': {'batch_completed': 1},
                    '$set': {'end_time': end_time}
                }
            )
        except Exception as e:
            print(e)

    def update_allocation_run_batch_audit(self, start_time, batch_id):
        """
        Update batch audit with start and end times
        """
        end_time = datetime.utcnow()
        try:
            collection = self.db['allocation_run_batch_audit']
            collection.update_one(
                {'batch_id': batch_id},
                {
                    '$set': {
                        'start_time': start_time,
                        'end_time': end_time
                    }
                }
            )
            print("Updated batch audit")
        except Exception as e:
            print(e)

    def get_shortest_run(self):
        """
        Get the run with the smallest remaining batch count
        """
        try:
            collection = self.db['allocation_run_audit']
            
            # MongoDB aggregation to find shortest run
            pipeline = [
                {
                    '$match': {
                        '$expr': {'$ne': [{'$subtract': ['$total_batch_size', '$batch_completed']}, 0]},
                        'adhoc_run_status': 'IN-QUEUE'
                    }
                },
                {
                    '$addFields': {
                        'remaining_batches': {'$subtract': ['$total_batch_size', '$batch_completed']},
                        'marketListCountInt': {'$toInt': '$marketListCount'}
                    }
                },
                {
                    '$sort': {
                        'remaining_batches': 1,
                        'marketListCountInt': 1
                    }
                },
                {
                    '$limit': 1
                }
            ]
            
            result = list(collection.aggregate(pipeline))
            
            if result:
                doc = result[0]
                # Return tuple similar to SQL: (run_id, batch_completed, total_batch_size)
                shortest_run = (doc['run_id'], doc['batch_completed'], doc['total_batch_size'])
                return shortest_run
            else:
                print("No shortest run found")
                sys.exit()
        except Exception as e:
            self.logger.error(e)
            print("Nothing to run")
            sys.exit()

    def check_event_schedule(self, rule_name):
        try:
            response = self.events.describe_rule(Name=rule_name)
            rule_details = response['Name']
            schedule_expression = response['ScheduleExpression']

            print(f"Rule Name: {rule_name}")
            print(f"Schedule Expression: {schedule_expression}")
        except self.events.exceptions.ResourceNotFoundException:
            print(f"Rule '{rule_name}' not found.")
            return None

        cron_expression = self.extract_cron_expression(schedule_expression)
        
        if self.get_time(cron_expression) == None:
            print("This is the next job time: ", self.get_next_run_time(cron_expression))
            return self.get_next_run_time(cron_expression)
        
        print("This is the next job time: ", self.get_time(cron_expression))
        return self.get_time(cron_expression)

    def smallest_batch(self, batch_names):
        run_ids = [batch_name.split('_batch_')[0] for batch_name in batch_names]

        run_id_counts = Counter(run_ids)

        most_batches_run_id = min(run_id_counts, key=run_id_counts.get)
        min_batches_count = run_id_counts[most_batches_run_id]

        print("Run_Id with the most number of batches:", most_batches_run_id)
        return most_batches_run_id

    def get_avg_run_time(self):  # TO BE USED LATER
        unitTimePerBatch = self.totalTime / (self.totalFlightDaysInRun / self.concurrency)
        return unitTimePerBatch

    def get_required_batch(self, run_details):
        batch_no = run_details[1] + 1
        req_run_id = "adhoc_stack_" + run_details[0] + "_batch_" + str(batch_no)
        return req_run_id

    def can_a_batch_run(self, next_run_time):
        if next_run_time >= self.threshold_time:
            smallest_batch_id = self.get_shortest_run()
            start_time = datetime.utcnow()
            required_batch_id = self.get_required_batch(smallest_batch_id)
            print("Processing batch: ", required_batch_id)
            
            temp_list_of_values = self.cache_client.get(required_batch_id)
            list_of_values = json.loads(temp_list_of_values)

            data_dict = list_of_values[0]
            queue_name = data_dict["queue"]
            
            for record in list_of_values:
                self.sqs.send_message(
                    QueueUrl=queue_name,
                    MessageBody=json.dumps(record)
                )

            self.update_allocation_run_batch_audit(start_time, required_batch_id)
            self.cache_client.delete(required_batch_id)
            
            if smallest_batch_id[2] == smallest_batch_id[1] + 1:
                self.update_run_end(smallest_batch_id[0])
            else:
                self.update_run_with_batches(smallest_batch_id[0])
            
            print("SENT")

            # Update adhoc_run_status to 'DONE' when all batches completed
            collection = self.db['allocation_run_audit']
            collection.update_one(
                {
                    'run_id': smallest_batch_id[0],
                    '$expr': {'$eq': ['$total_batch_size', '$batch_completed']}
                },
                {'$set': {'adhoc_run_status': 'DONE'}}
            )