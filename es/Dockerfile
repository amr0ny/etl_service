FROM elasticsearch:8.6.2

WORKDIR /opt/es
ENV xpack.security.enabled=false
ENV discovery.type=single-node
ENV ES_JAVA_OPTS="-Xms200m -Xmx200m"

COPY es_index.sh .

RUN chmod +x .
