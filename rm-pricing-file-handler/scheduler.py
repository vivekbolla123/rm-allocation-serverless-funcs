# from apscheduler.schedulers.blocking import BlockingScheduler
# from main import FileHandler

# # Create a scheduler
# sched = BlockingScheduler()
# file_handler = FileHandler()

# sched.add_job(
#     file_handler.get_s3_files_and_upload, 
#     'cron', 
#     second=5,
#     max_instances=1,
#     misfire_grace_time=4
# )

# # Start the scheduler
# print("Scheduler started.")
# sched.start()