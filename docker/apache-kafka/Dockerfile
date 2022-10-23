FROM openjdk:11.0.6-jre-buster

# build time
ARG ZOOKEEPER_ADDRESS=zookeeper:2181

# runtime
ENV KAFKA_DIR=/opt/kafka
ENV ZOOKEEPER_ADDRESS="${ZOOKEEPER_ADDRESS}"
RUN mkdir -p ${KAFKA_DIR}
RUN apt-get update
RUN apt-get install curl -y
RUN curl -o kafka.tgz https://downloads.apache.org/kafka/3.3.1/kafka_2.13-3.3.1.tgz
RUN tar -xzf kafka.tgz -C /opt/kafka --strip-components=1
WORKDIR ${KAFKA_DIR}
COPY kafka-startup.sh ${KAFKA_DIR}
RUN chmod +x kafka-startup.sh

CMD bash kafka-startup.sh
