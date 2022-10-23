FROM openjdk:11.0.6-jre-buster

# build time
ARG ZOOKEEPER_ADDRESS=zookeeper:2181

# runtime
ENV KAFKA_DIR=/opt/kafka
#ENV KAFKA_DIR="${KAFKA_DIR}"
ENV ZOOKEEPER_ADDRESS="${ZOOKEEPER_ADDRESS}"
RUN mkdir -p ${KAFKA_DIR}

WORKDIR ${KAFKA_DIR}
COPY kafka-startup.sh ${KAFKA_DIR}
COPY kafka_2.13-3.3.1 ${KAFKA_DIR}
RUN chmod +x kafka-startup.sh

CMD bash kafka-startup.sh
