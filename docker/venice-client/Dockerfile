# syntax=docker/dockerfile:1
FROM  mcr.microsoft.com/openjdk/jdk:11-ubuntu

ARG ADMIN_JAR="clients/venice-admin-tool/build/libs/venice-admin-tool-all.jar"
ARG VPJ_JAR="clients/hadoop-to-venice-bridge/build/libs/hadoop-to-venice-bridge-all.jar"
ARG THIN_CLI_JAR="clients/venice-thin-client/build/libs/venice-thin-client-all.jar"
ARG VPJ_PROPS_FILES="docker/venice-client/configs"

ARG H1="hadoop-common-2.3.0.jar"
ARG H2="hadoop-mapreduce-client-common-2.3.0.jar"
ARG H3="hadoop-mapreduce-client-core-2.3.0.jar"

# env vars used inside dockerq
ENV VENICE_DIR="/opt/venice"

# create directories inside docker image
RUN mkdir $VENICE_DIR
RUN mkdir -p "$VENICE_DIR/configs"
WORKDIR $VENICE_DIR

COPY $H1 $VENICE_DIR
COPY $H2 $VENICE_DIR
COPY $H3 $VENICE_DIR
# copy jar and properties file
COPY $ADMIN_JAR $VENICE_DIR
COPY $VPJ_JAR $VENICE_DIR
COPY $THIN_CLI_JAR $VENICE_DIR
COPY $VPJ_PROPS_FILES "$VENICE_DIR/configs/"

# entrypoint
CMD /bin/sh -c bash



