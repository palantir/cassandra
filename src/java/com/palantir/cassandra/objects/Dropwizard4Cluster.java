package com.palantir.cassandra.objects;

import com.codahale.metrics.jmx.JmxReporter;
import com.datastax.driver.core.Cluster;

public class Dropwizard4Cluster extends Cluster {
    private final JmxReporter reporter;

    private Dropwizard4Cluster(Builder builder) {
        super(builder);
        this.reporter = JmxReporter.forRegistry(this.getMetrics().getRegistry())
                .inDomain(this.getClusterName() + "-metrics")
                .build();
    }

    @Override
    public void close() {
        reporter.close();
        super.close();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends Cluster.Builder {
        @Override
        public Cluster build() {
            return new Dropwizard4Cluster(this);
        }
    }
}
