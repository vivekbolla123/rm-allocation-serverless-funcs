FROM public.ecr.aws/lambda/python:3.10

ARG ENVN
ENV env $ENVN

RUN echo "Creating image for $env Environment"

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY ./ ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler
CMD [ "lambda_function.lambda_handler" ]
