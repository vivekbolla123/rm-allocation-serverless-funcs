FROM public.ecr.aws/lambda/python:3.10

ARG ENVN
ENV env $ENVN

RUN echo "Creating image for $env Environment"
# Install the function's dependencies using file requirements.txt
# from your project folder.
RUN yum install -y gcc
RUN yum install -y mysql-devel

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}
COPY configsettings.py ${LAMBDA_TASK_ROOT}
COPY pushRBDToSFTP.py ${LAMBDA_TASK_ROOT}
COPY allocation_summary.py ${LAMBDA_TASK_ROOT}
COPY runConnections.py ${LAMBDA_TASK_ROOT}
COPY markRunCompleted.py ${LAMBDA_TASK_ROOT}
COPY cleanup.py ${LAMBDA_TASK_ROOT}
COPY scheduled_job_run_audit.py ${LAMBDA_TASK_ROOT}
COPY sftp_error_rate.py ${LAMBDA_TASK_ROOT}
COPY pushRBDToS3.py ${LAMBDA_TASK_ROOT}
COPY daily_rolling.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ] 