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
        
        if self.cache_client.ping():
            self.logger.info("Connected to Redis")

        # main code
        scheduler_names = self.get_scheduler_names()
        if not scheduler_names:
            self.logger.warning("No scheduler names found. Exiting gracefully.")
            return

        xz = self.get_shortest_run()
        if xz is None:
            self.logger.info("No adhoc runs in queue.")
            return

        dic = {}
        for i in scheduler_names:
            temp_time = self.check_event_schedule(i)
            if temp_time is not None:
                dic[str(temp_time)] = i

        if not dic:
            self.logger.warning("No valid scheduler schedules found. Exiting gracefully.")
            return

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
            # self.logger.info(f"Scheduler Names: {dic}")
            # Get current UTC time
            current_time_utc = datetime.now()
            # self.logger.info(f"Current time: {current_time_utc}")
            time_obj = datetime.strptime(dic_keys[0], '%Y-%m-%d %H:%M:%S')
            time_diff = time_obj - current_time_utc
            total_minutes = time_diff.total_seconds() // 60
            # self.logger.info(f"Time difference for {dic_keys[0]}: {total_minutes} minutes")
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
            self.logger.error(f"Error getting next run time: {e}")
            return None

    def get_time(self, quartz_expression):
        try:
            aws_cron = AWSCron(quartz_expression)
            current_time = datetime.now()
            next_occurrence = aws_cron.occurrence(current_time).next()
            return next_occurrence.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            self.logger.error(f"Error parsing AWS cron expression: {e}")
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
            self.logger.info(f"Scheduler names: {scheduler_names}")
        except Exception as e:
            self.logger.error(f"Failed to fetch scheduler names: {e}")
            return []

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
            self.logger.error(f"Failed to update batch count for run {run_id}: {e}")

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
            self.logger.error(f"Failed to update run end for {run_id}: {e}")

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
            self.logger.info(f"Updated batch audit for {batch_id}")
        except Exception as e:
            self.logger.error(f"Failed to update batch audit for {batch_id}: {e}")

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
                shortest_run = (doc['run_id'], doc['batch_completed'], doc['total_batch_size'])
                return shortest_run
            else:
                self.logger.warning("No shortest run found")
                return None
        except Exception as e:
            self.logger.error(f"Error fetching shortest run: {e}")
            return None

    def check_event_schedule(self, rule_name):
        try:
            response = self.events.describe_rule(Name=rule_name)
            rule_details = response['Name']
            schedule_expression = response['ScheduleExpression']
            self.logger.info(f"Rule Name: {rule_name}, Schedule Expression: {schedule_expression}")
        except self.events.exceptions.ResourceNotFoundException:
            self.logger.warning(f"Rule '{rule_name}' not found.")
            return None

        cron_expression = self.extract_cron_expression(schedule_expression)
        
        next_time = self.get_time(cron_expression)
        if next_time is None:
            next_time = self.get_next_run_time(cron_expression)

        self.logger.info(f"Next job time for {rule_name}: {next_time}")
        return next_time

    def smallest_batch(self, batch_names):
        run_ids = [batch_name.split('_batch_')[0] for batch_name in batch_names]

        run_id_counts = Counter(run_ids)

        most_batches_run_id = min(run_id_counts, key=run_id_counts.get)
        min_batches_count = run_id_counts[most_batches_run_id]
        self.logger.info(f"Run_Id with the fewest remaining batches: {most_batches_run_id}")
        return most_batches_run_id

    def mark_run_done_if_complete(self, run_id):
        """
        Mark adhoc_run_status as DONE if all batches have been completed
        """
        try:
            collection = self.db['allocation_run_audit']
            collection.update_one(
                {
                    'run_id': run_id,
                    '$expr': {'$eq': ['$total_batch_size', '$batch_completed']}
                },
                {'$set': {'adhoc_run_status': 'DONE'}}
            )
        except Exception as e:
            self.logger.error(f"Failed to mark run {run_id} as DONE: {e}")

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
            if smallest_batch_id is None:
                self.logger.warning("No adhoc runs in queue. Skipping batch.")
                return

            start_time = datetime.utcnow()
            required_batch_id = self.get_required_batch(smallest_batch_id)
            self.logger.info(f"Processing batch: {required_batch_id}")
            
            temp_list_of_values = self.cache_client.get(required_batch_id)

            if temp_list_of_values is None:
                self.logger.warning(f"Batch key '{required_batch_id}' not found in Redis — already processed or expired. Skipping.")
                self.update_run_with_batches(smallest_batch_id[0])
                self.mark_run_done_if_complete(smallest_batch_id[0])
                return

            list_of_values = json.loads(temp_list_of_values)

            if not list_of_values:
                self.logger.warning(f"Batch key '{required_batch_id}' has empty data in Redis. Skipping.")
                self.update_run_with_batches(smallest_batch_id[0])
                self.mark_run_done_if_complete(smallest_batch_id[0])
                return

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

            self.logger.info(f"Batch {required_batch_id} sent to SQS")
            self.mark_run_done_if_complete(smallest_batch_id[0])
