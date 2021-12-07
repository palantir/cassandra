/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.metrics;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;


public class ClientMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("Client");
    public static final ClientMetrics instance = new ClientMetrics();

    private ConcurrentHashMap<String, Meter> meters;

    private ClientMetrics()
    {
        meters = new ConcurrentHashMap<>();
    }

    public void addCounter(String name, final Callable<Integer> provider)
    {
        Metrics.register(factory.createMetricName(name), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                try
                {
                    return provider.call();
                } catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public Meter getRequestsInvolvingKeyspaceMeter(String prefix, String keyspace)
    {
        String name = prefix + "Requests,keyspace=" + keyspace;
        if (!meters.containsKey(name))
        {
            meters.put(name, Metrics.meter(factory.createMetricName(name)));
        }
        return meters.get(name);
    }
}
