FROM cassandra:3.11

ARG BUILD_VERSION
ENV CASSANDRA_VERSION $BUILD_VERSION
ENV CASSANDRA_DIR palantir-cassandra-$BUILD_VERSION
ENV PATH $CASSANDRA_DIR/bin:$PATH

# add and untar the Palantir Cassandra distribution
ADD palantir-cassandra-*tgz /

ENV _JAVA_OPTIONS=-Dcassandra.skip_wait_for_gossip_to_settle=0

ENV CASSANDRA_COMMAND cassandra
ENV CASSANDRA_CONFIG $CASSANDRA_DIR/conf/
ENV CASSANDRA_YAML $CASSANDRA_CONFIG/cassandra.yaml
ENV CASSANDRA_ENV $CASSANDRA_CONFIG/cassandra-env.sh

COPY cassandra.yaml $CASSANDRA_YAML
COPY cassandra-env.sh $CASSANDRA_ENV

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN ["chmod", "+x", "/docker-entrypoint.sh"]

RUN ["sh", "-c", "chmod +rwx ${CASSANDRA_DIR}"]
RUN ["sh", "-c", "chown cassandra:cassandra ${CASSANDRA_DIR}"]

EXPOSE 7000 7001 7199 9042 9160
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["cassandra", "-f"]
