FROM openjdk:11.0.6-jre-buster

ARG SRC_ZK_JAR="zookeeper-3.9.0-SNAPSHOT-fatjar.jar"
ARG SRC_ZK_CFG="zoo.cfg"

ENV ZK_DIR=/opt/zookeeper
ENV ZK_JAR="${ZK_DIR}/zookeeper-3.9.0-all.jar"
ENV ZK_CFG="${ZK_DIR}/zoo.cfg"

RUN apt-get update
RUN apt-get install netcat -y
RUN mkdir -p ${ZK_DIR}
RUN mkdir -p "${ZK_DIR}/data"

WORKDIR ${ZK_DIR}
COPY ${SRC_ZK_JAR} ${ZK_JAR}
COPY ${SRC_ZK_CFG} ${ZK_CFG}

CMD java -jar ${ZK_JAR} server ${ZK_CFG}
