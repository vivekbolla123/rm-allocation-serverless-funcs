import json
import configsettings
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone
from constants_utils import ConstantsUtils
import traceback
import boto3
from aws_croniter import AwsCroniter
from aws_croniter.exceptions import AwsCroniterExpressionError
import warnings
warnings.filterwarnings('ignore')

class ScheduledRunFailureAlert:

    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        self.navconn = params['navconn']
        self.mongoconn = params['mongoconn'] 
        self.db = self.mongoconn['qp_dw_rmalloc'] 
        self.db_reports = self.mongoconn['qp_rm_reports'] 
        self.cache_client = params['cache_client']
        self.constants = ConstantsUtils(self.mongoconn['qp_dw_rmalloc'])

    def run(self):
        try:
            self.logger.info("Starting the run method.")

            # ------------------------------------------------------
            # Get run data from DB (previous_hour_runs already
            # contains success_percent logic)
            # ------------------------------------------------------
            last_hour_runs, previous_hour_runs = self.get_runs_hourly_windows()

            failed_runs = [ doc for doc in previous_hour_runs if doc.get("success_percent") in (None, 0) ]
            missing_runs = []

            # ------------------------------------------------------
            # Load schedulers + cron hits + extracted input constants
            # ------------------------------------------------------
            schedulers = self.get_scheduler_cron_hits()
            WINDOW_MINUTES = 10   

            # ------------------------------------------------------
            # Compare cron hits vs DB runs
            # ------------------------------------------------------
            for scheduler in schedulers:
                cron_hits = scheduler["hits_in_window"]

                region = scheduler.get("region")
                mkt = scheduler.get("marketListName")
                run_type = scheduler.get("type")
                dtd_start = scheduler.get("dtd_start")
                dtd_end = scheduler.get("dtd_end")

                for hit in cron_hits:

                    matching_found = False
                    window_delta = timedelta(minutes=WINDOW_MINUTES)

                    # Iterate over DB runs directly instead of rounded-index
                    for db_run in last_hour_runs:
                        db_dt = db_run.get("start_dt")
                        if not db_dt:
                            continue

                        # ---------- TIME WINDOW MATCH ----------
                        # Cron hit = 12:15
                        # DB start_time = 12:18 → OK if <= 5 min difference
                        if abs(db_dt - hit) > window_delta:
                            continue

                        # ---------- FILTER MATCHES ----------
                        if region and db_run.get("region") != region:
                            continue

                        if mkt and db_run.get("market_list_name") != mkt:
                            continue

                        if run_type and db_run.get("type") != run_type:
                            continue

                        if dtd_start and db_run.get("dtd_start") != dtd_start:
                            continue

                        if dtd_end and db_run.get("dtd_end") != dtd_end:
                            continue

                        # All matched → valid run found
                        matching_found = True
                        break

                    if not matching_found:
                        missing_runs.append({
                            "run_id": "",
                            "user": "SYS2",
                            "start_time": hit.strftime("%Y-%m-%d %H:%M:%S"),
                            "region": region,
                            "market_list_name": mkt,
                            "type": run_type,
                            "dtd_start": dtd_start,
                            "dtd_end": dtd_end,
                            "start_dt": db_dt
                        })

            if failed_runs:
                self.compose_and_send_email("RM Alert: Scheduled Run Not Completed", failed_runs)
                self.logger.info("Sent Alert mail for failed runs")
            
            if missing_runs:
                self.compose_and_send_email("RM Alert: Scheduled Run Not Started", missing_runs)
                self.logger.info("Sent Alert mail for runs that failed to start")

            self.logger.info("Run method completed.")

        except Exception:
            self.logger.error(f"An error occurred in the run method: {traceback.format_exc()}")

    def get_runs_hourly_windows(self):
        runs_col = self.db['allocation_run_audit']
        summary_col = self.db_reports['allocation_summary_report']

        now_utc = datetime.now(timezone.utc)

        # Define hour boundaries
        hour_current = now_utc.replace(minute=0, second=0, microsecond=0)
        hour_prev = hour_current - timedelta(hours=1)
        hour_prev2 = hour_current - timedelta(hours=2)

        # Convert boundaries to string timestamps
        ts_current = hour_current.strftime("%Y-%m-%d %H:%M:%S")
        ts_prev = hour_prev.strftime("%Y-%m-%d %H:%M:%S")
        ts_prev2 = hour_prev2.strftime("%Y-%m-%d %H:%M:%S")

        # Fetch only last 2 hours of runs
        cursor = runs_col.find(
            {
                'type': {'$in': ["Scheduled", "Scheduled_Connections"]},
                'start_time': {'$gte': ts_prev2}
            },
            {
                'run_id': 1,
                'type': 1,
                'region': 1,
                'user': 1,
                'market_list_name': 1,
                'start_time': 1,
                'dtd_start': 1,
                'dtd_end': 1,
                '_id': 0
            }
        )

        run_docs = list(cursor)

        # Hour buckets
        last_hour_runs = []       # prev → current
        previous_hour_runs = []   # prev2 → prev

        for doc in run_docs:
            try:
                doc["start_dt"] = datetime.strptime(doc["start_time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except:
                doc["start_dt"] = None

            if hour_prev <= doc["start_dt"] < hour_current:
                last_hour_runs.append(doc)
            elif hour_prev2 <= doc["start_dt"] < hour_prev:
                previous_hour_runs.append(doc)

        # Join summary fields for last hour only
        if previous_hour_runs:
            run_ids = [doc["run_id"] for doc in previous_hour_runs]

            summary_docs = list(
                summary_col.aggregate([
                    {
                        "$match": {
                            "run_id": {"$in": run_ids}
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "run_id": 1,
                            "failures": 1,
                            "total": 1,
                            "success_percent": {
                                "$cond": [
                                    {
                                        "$or": [
                                            {"$eq": ["$total", None]},  
                                            {"$eq": ["$total", 0]},
                                            {"$eq": ["$failures", None]}
                                        ]
                                    },
                                    0,  # return 0 if fields missing
                                    {
                                        "$multiply": [
                                            {
                                                "$subtract": [
                                                    1,
                                                    {"$divide": ["$failures", "$total"]}
                                                ]
                                            },
                                            100
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                ])
            )

            summary_map = {d["run_id"]: d for d in summary_docs}

            for doc in previous_hour_runs:
                rid = doc["run_id"]
                summary = summary_map.get(rid, {})
                doc["success_percent"] = summary.get("success_percent")

        return last_hour_runs, previous_hour_runs

    def get_schedulers(self):
        collection = self.db["config_schedulers"]

        cursor = collection.find(
            {
                "allocationType": {"$in": ["I", "D"]}
            },
            {
                "schedulerName": 1,
                "_id": 0
            }
        )

        list_obj = list(cursor)
        return list_obj
    
    def get_scheduler_cron_hits(self):
        """
        Fetch ALL enabled cron EventBridge rules that target this Lambda,
        extract input constants (only existing keys), and compute cron hits
        in the last hour window.
        """

        events = boto3.client("events")
        lambda_client = boto3.client("lambda")

        response = lambda_client.get_function(FunctionName=configsettings.FETCH_INPUTS_LAMBDA)
        lambda_arn = response["Configuration"]["FunctionArn"]

        # ------------------------------------------------------
        # Time window (UTC)
        # ------------------------------------------------------
        now_utc = datetime.now(timezone.utc)
        hour_current = now_utc.replace(minute=0, second=0, microsecond=0)
        hour_prev = hour_current - timedelta(hours=1)

        # ------------------------------------------------------
        # Find which Lambda function is running this job
        # ------------------------------------------------------
        # Get all rule names attached to this Lambda
        rule_names_resp = events.list_rule_names_by_target(TargetArn=lambda_arn)
        rule_names = rule_names_resp.get("RuleNames", [])

        results = []

        for rule_name in rule_names:

            # ------------------------------------------------------
            # 1) Describe rule
            # ------------------------------------------------------
            try:
                rule = events.describe_rule(Name=rule_name)
            except Exception:
                continue

            # SKIP disabled rules
            if rule.get("State") != "ENABLED":
                continue

            cron_expr = rule.get("ScheduleExpression", "")
            if not cron_expr.startswith("cron("):
                continue

            cron_pattern = cron_expr[5:-1]

            # ------------------------------------------------------
            # 2) Get Input JSON
            # ------------------------------------------------------
            input_json_raw = None
            try:
                trg = events.list_targets_by_rule(Rule=rule_name)
                if trg.get("Targets"):
                    input_json_raw = trg["Targets"][0].get("Input")
            except Exception:
                input_json_raw = None

            # defaults first
            extracted = {
                "dtd_start": 30,
                "dtd_end": 90,
                "updateNavType": "api",
                "marketListName": "market_list",
                "region": "Domestic",
                "type": "Adhoc"
            }

            if input_json_raw:
                try:
                    inp = json.loads(input_json_raw)
                    body_str = inp.get("body")

                    if body_str:
                        body = json.loads(body_str)

                        extracted["dtd_start"] = body.get("start_interval", extracted["dtd_start"])
                        extracted["dtd_end"] = body.get("end_interval", extracted["dtd_end"])
                        extracted["updateNavType"] = body.get("update_navitaire_method", extracted["updateNavType"])
                        extracted["marketListName"] = body.get("market_list_table_name", extracted["marketListName"])
                        extracted["region"] = body.get("region_method", extracted["region"])
                        extracted["type"] = body.get("run_type", extracted["type"])

                except Exception:
                    pass

            # ------------------------------------------------------
            # 3) Compute cron hits in last hour
            # ------------------------------------------------------
            cron_hits = []

            try:
                itr = AwsCroniter(cron_pattern)
                
                t = itr.get_prev(hour_current)[0]
                
                # Iterate backwards until outside time window
                while t >= hour_prev:
                    cron_hits.append(t)
                    t = itr.get_prev(t)[0]  
                    
            except AwsCroniterExpressionError as cron_e:
                self.logger.error(cron_e)
            except Exception as e:
                self.logger.error(f"Failed to get cron hit time: {e}")

            # ------------------------------------------------------
            # Add to results
            # ------------------------------------------------------
            if cron_hits:
                results.append({
                    "schedulerName": rule_name,
                    "cron": cron_expr,
                    "hits_in_window": cron_hits,
                    **extracted
                })

        return results
    
    def compose_and_send_email(self, subject, run_list):
        IST = timezone(timedelta(hours=5, minutes=30))

        # Start HTML table
        html_lines = ["""
        <html>
        <body>
            <table border="1" cellpadding="8" cellspacing="0">
                <tr>
                    <th>#</th>
                    <th>Run ID</th>
                    <th>Start Time (IST)</th>
                    <th>User</th>
                    <th>DTD Start</th>
                    <th>DTD End</th>
                    <th>Type</th>
                    <th>Market List</th>
                    <th>Region</th>
                </tr>
        """]
        
        # Add rows
        for i, record in enumerate(run_list, 1):
            start_time_ist = record['start_dt'].astimezone(IST).strftime('%Y-%m-%d %H:%M:%S')
            html_lines.append(f"""
                <tr>
                    <td>{i}</td>
                    <td>{record['run_id']}</td>
                    <td>{start_time_ist}</td>
                    <td>{record['user']}</td>
                    <td>{record['dtd_start']}</td>
                    <td>{record['dtd_end']}</td>
                    <td>{record['type']}</td>
                    <td>{record['market_list_name']}</td>
                    <td>{record['region']}</td>
                </tr>
            """)
        
        # Close HTML table
        html_lines.append("""
            </table>
        </body>
        </html>
        """)
        
        mail_body = ''.join(html_lines)
        
        self.send_email_alert(subject, mail_body, self.constants.RM_ALERT_MAIL_ID, is_html=True)
        # self.send_email_alert(subject, mail_body, "aarav.dubey@akasaair.com", is_html=True)

    def send_email_alert(self, subject, body, recipient, is_html=False):
        # Create message container
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = configsettings.SENDER_EMAIL
        msg['To'] = recipient

        # Attach body
        if is_html:
            msg.attach(MIMEText(body, 'html'))
        else:
            msg.attach(MIMEText(body, 'plain'))

        # Send the email
        try:
            with smtplib.SMTP(configsettings.SMTP_SERVER, configsettings.SMTP_PORT) as server:
                server.starttls()  # Enable TLS
                server.login(configsettings.SENDER_EMAIL, configsettings.SENDER_PASSWORD)
                server.sendmail(configsettings.SENDER_EMAIL, recipient, msg.as_string())
            self.logger.info(f"Email sent to {recipient} successfully.")
        except Exception as e:
            self.logger.error(f"Failed to send email: {e}")
