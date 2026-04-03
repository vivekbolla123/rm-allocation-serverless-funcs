FROM public.ecr.aws/lambda/python:3.10
ARG ENVN
ARG DD_SERVICE_NAME
ENV env $ENVN
RUN echo "Creating image for $env Environment"

# Install the function's dependencies using file requirements.txt from your project folder.
RUN yum install -y gcc
RUN yum install -y mysql-devel
COPY requirements.txt  .
RUN pip3 install datadog-lambda --target "${LAMBDA_TASK_ROOT}"
COPY --from=public.ecr.aws/datadog/lambda-extension:latest /opt/. /opt/
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY ./ ${LAMBDA_TASK_ROOT}/

# DATADOG CONFIGURATION START
ENV DD_SERVICE=${DD_SERVICE_NAME}
CMD ["datadog_lambda.handler.handler"]
ENV DD_LAMBDA_HANDLER=lambda_function.lambda_handler
ENV DD_SITE=datadoghq.com
ENV DD_TRACE_ENABLED=true
ENV DD_LOGS_INJECTION=true
ENV DD_ENV=$ENVN
ENV DD_VERSION=1.0.0