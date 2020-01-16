FROM cassandra:2.2

ARG BUILD_VERSION

ENV CASSANDRA_VERSION $BUILD_VERSION
ENV CASSANDRA_DIR palantir-cassandra-$BUILD_VERSION
ENV PATH $CASSANDRA_DIR/bin:$PATH

# Untar our distribution
ADD palantir-cassandra-*tgz /

# We give the cassandra user permissions to create things in the Cassandra dir
RUN ["sh", "-c", "chmod +rwx ${CASSANDRA_DIR}"]
RUN ["sh", "-c", "chown cassandra:cassandra ${CASSANDRA_DIR}"]

# Configs set up
ENV CASSANDRA_CONFIG $CASSANDRA_DIR/conf/
ENV CASSANDRA_YAML $CASSANDRA_CONFIG/cassandra.yaml
ENV CASSANDRA_ENV $CASSANDRA_CONFIG/cassandra-env.sh
COPY cassandra.yaml $CASSANDRA_YAML
COPY cassandra-env.sh $CASSANDRA_ENV

ENV _JAVA_OPTIONS=-Dcassandra.skip_wait_for_gossip_to_settle=0

# Enable wide JMX access for tests, copied from https://stackoverflow.com/questions/48007037/run-remote-nodetool-commands-on-cassandra-3-without-authentication
RUN sed -i s/'jmxremote.authenticate=true'/'jmxremote.authenticate=false'/g $CASSANDRA_ENV
RUN sed -i s/'LOCAL_JMX=yes'/'LOCAL_JMX=no'/g $CASSANDRA_ENV
RUN sed -i s/'JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.password.file'/'#JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.password.file'/g $CASSANDRA_ENV
RUN sed -i s/'JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.access.file'/'#JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.access.file'/g $CASSANDRA_ENV

# Entry set up
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN ["chmod", "+x", "/docker-entrypoint.sh"]

EXPOSE 7000 7001 7199 9042 9160
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["cassandra", "-f"]`
