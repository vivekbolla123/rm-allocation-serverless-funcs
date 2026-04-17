FROM public.ecr.aws/lambda/python:3.10

ARG ENVN
ENV env $ENVN

RUN echo "Creating image for $env Environment"
# Install the function's dependencies using file requirements.txt
# from your project folder.
COPY requirements.txt  .
RUN yum install -y gcc freetds freetds-devel && \
    pip3 install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}" && \
    yum remove -y gcc freetds-devel && \
    yum clean all && \
    rm -rf /var/cache/yum

# Copy function code
COPY ./ ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ] 