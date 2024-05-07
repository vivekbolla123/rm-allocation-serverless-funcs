FROM public.ecr.aws/lambda/java:21

ARG ENVN
ENV env $ENVN 

ARG RM_DB_URL 
ARG RM_DB_USERNAME 
ARG FILE_PATH_NAME
ARG S3_BUCKET_NAME 
ARG RM_DB_PASSWORD

ENV RM_DB_URL $RM_DB_URL
ENV RM_DB_USERNAME $RM_DB_USERNAME
ENV FILE_PATH_NAME $FILE_PATH_NAME
ENV S3_BUCKET_NAME $S3_BUCKET_NAME
ENV RM_DB_PASSWORD $RM_DB_PASSWORD

RUN echo "Creating image for $env Environment"

# Copy function code and runtime dependencies from Maven layout
COPY target/classes ${LAMBDA_TASK_ROOT}
COPY target/dependency/* ${LAMBDA_TASK_ROOT}/lib/

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "com.akasaair.queues.App::handleRequest" ]