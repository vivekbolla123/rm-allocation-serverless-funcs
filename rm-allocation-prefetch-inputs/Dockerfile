FROM public.ecr.aws/lambda/python:3.10

ARG ENVN
ENV env $ENVN

COPY requirements.txt  .
RUN yum install -y gcc freetds freetds-devel && \
    pip3 install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}" && \
    find "${LAMBDA_TASK_ROOT}" -type d \( -name "tests" -o -name "test" \) -exec rm -rf {} + || true && \
    find "${LAMBDA_TASK_ROOT}" -type d -name "__pycache__" -exec rm -rf {} + || true && \
    rm -rf "${LAMBDA_TASK_ROOT}/boto3"* "${LAMBDA_TASK_ROOT}/botocore"* && \
    yum remove -y gcc freetds-devel && \
    yum clean all && \
    rm -rf /var/cache/yum

# Copy function code
COPY ./ ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler
CMD [ "lambda_function.lambda_handler" ]
