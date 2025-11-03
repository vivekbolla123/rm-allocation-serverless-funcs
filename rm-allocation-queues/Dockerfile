# # Stage 1: Build
# FROM maven:3.9-eclipse-temurin-21 AS build
# WORKDIR /app

# COPY pom.xml .
# # RUN mvn dependency:go-offline -B

# COPY src ./src
# RUN mvn clean package -DskipTests

# # Stage 2: Runtime
# FROM eclipse-temurin:21-jre-alpine
# WORKDIR /app

# # Environment variables
# ARG ENVN
# ENV env=$ENVN
# ENV SPRING_PROFILES_ACTIVE=$ENVN
# # Home dir for DuckDB
# ENV HOME=/tmp

# RUN echo "Creating image for $env Environment"

# COPY /target/*.jar app.jar

# EXPOSE 8080

# ENTRYPOINT ["java", "-jar", "app.jar"]