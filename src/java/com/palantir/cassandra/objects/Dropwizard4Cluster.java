package com.palantir.cassandra.objects;

import com.codahale.metrics.jmx.JmxReporter;
import com.datastax.driver.core.Cluster;

// Necessary for upgrading from Dropwizard 3.x to Dropwizard 4.x
// See https://docs.datastax.com/en/developer/java-driver/3.6/manual/metrics/index.html#metrics-4-compatibility
public class Dropwizard4Cluster extends Cluster {
    private JmxReporter reporter;

    private Dropwizard4Cluster(Cluster.Builder builder) {
        super(builder);
        try {
            this.reporter = JmxReporter.forRegistry(this.getMetrics().getRegistry())
                    .inDomain(this.getClusterName() + "-metrics")
                    .build();
        } catch (NullPointerException e) {
            // This is a workaround for yet unknown issue in the integration tests
            this.reporter = null;
        }
    }

    @Override
    public void close() {
        if (reporter != null) {
            reporter.close();
        }
        super.close();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends Cluster.Builder {
        @Override
        public Cluster build() {
            return new Dropwizard4Cluster(this.withoutJMXReporting());
        }
    }
}
