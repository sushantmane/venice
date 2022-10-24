FROM  mcr.microsoft.com/openjdk/jdk:11-ubuntu

ENV VENICE_DIR=/opt/venice

RUN apt-get update
RUN apt-get install tree
RUN mkdir -p ${VENICE_DIR}/bin && mkdir -p ${VENICE_DIR}/configs


WORKDIR ${VENICE_DIR}
COPY single-dc-configs configs/single-dc
COPY multi-dc-configs configs/multi-dc
COPY venice-controller-all.jar bin/venice-controller-all.jar

CMD java -jar bin/venice-controller-all.jar "$VENICE_DIR/configs/cluster.properties" "$VENICE_DIR/configs/controller.properties"
