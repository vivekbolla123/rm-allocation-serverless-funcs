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
COPY ./models ${LAMBDA_TASK_ROOT}/models/
COPY ./utilities ${LAMBDA_TASK_ROOT}/utilities/
COPY ./helper ${LAMBDA_TASK_ROOT}/helper/

COPY determine_rbd.py ${LAMBDA_TASK_ROOT}
COPY get_dynamic_time_d2.py ${LAMBDA_TASK_ROOT}
COPY market_fares_service.py ${LAMBDA_TASK_ROOT}
COPY ownlogger.py ${LAMBDA_TASK_ROOT}
COPY lambda_function.py ${LAMBDA_TASK_ROOT}
COPY Constants.py ${LAMBDA_TASK_ROOT}
COPY runAlloc.py ${LAMBDA_TASK_ROOT}
COPY configsettings.py ${LAMBDA_TASK_ROOT}
COPY allocations.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ] 