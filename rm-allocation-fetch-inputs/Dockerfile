FROM public.ecr.aws/lambda/python:3.10

ARG ENVN
ENV env $ENVN

RUN echo "Creating image for $env Environment"
# Install the function's dependencies using file requirements.txt
# from your project folder.
RUN yum install -y gcc
RUN yum install -y openssl-devel-1.0.2k-24.amzn2.0.14
RUN yum install -y mysql-devel

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY ./ ${LAMBDA_TASK_ROOT}/


# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ] 