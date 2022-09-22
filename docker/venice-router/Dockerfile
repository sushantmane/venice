# syntax=docker/dockerfile:1
FROM  mcr.microsoft.com/openjdk/jdk:11-ubuntu

ARG JAR_SRC="services/venice-router/build/libs/venice-router-all.jar"
ARG PROPS_FILES="docker/venice-router/configs/"

# env vars used inside docker
ENV VENICE_DIR="/opt/venice"
ENV JAR_DEST="$VENICE_DIR/venice-router-all.jar"

# create directories inside docker image
RUN mkdir $VENICE_DIR
RUN mkdir -p "$VENICE_DIR/configs"
WORKDIR $VENICE_DIR

# copy jar and properties file
COPY $JAR_SRC $JAR_DEST
COPY $PROPS_FILES "$VENICE_DIR/configs/"

# entrypoint
CMD java -jar $JAR_DEST "$VENICE_DIR/configs/router.properties"