FROM public.ecr.aws/lambda/python:3.10
ARG ENVN
ARG GIT_COMMIT_SHA
ARG GIT_BRANCH
ARG GIT_TAG
ENV env=$ENVN
RUN echo "Creating image for $env Environment"

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY --from=public.ecr.aws/datadog/lambda-extension:latest /opt/. /opt/

COPY ./ ${LAMBDA_TASK_ROOT}/

# Datadog configuration
# DD_API_KEY set in configsettings.py
ENV DD_LAMBDA_HANDLER=lambda_function.lambda_handler
ENV DD_SITE=datadoghq.com
ENV DD_TRACE_ENABLED=true
ENV DD_SERVERLESS_LOGS_ENABLED=true
ENV DD_LOGS_INJECTION=true
ENV DD_COLD_START_TRACING=false
ENV DD_ENV=${ENVN}
ENV DD_SERVICE=rm-prefetch-inputs
ENV DD_VERSION=${GIT_TAG}
ENV DD_TAGS="git.commit.sha:${GIT_COMMIT_SHA},git.branch:${GIT_BRANCH},Department:RM"

CMD ["datadog_lambda.handler.handler"]
