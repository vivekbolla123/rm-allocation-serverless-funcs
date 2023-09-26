FROM public.ecr.aws/lambda/python:3.10

# Install the function's dependencies using file requirements.txt
# from your project folder.
RUN yum install -y gcc
RUN yum install -y mysql-devel

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}
COPY configsettings.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ] 