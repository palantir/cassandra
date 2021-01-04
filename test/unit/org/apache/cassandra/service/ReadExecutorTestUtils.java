/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.MockSchema;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.metrics.PredictedSpeculativeRetryPerformanceMetrics;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ReadExecutorTestUtils {

    private ReadExecutorTestUtils() {}

    public static AbstractReadExecutor getTestNeverSpeculatingReadExecutor(String keyspace, String cf, List<InetAddress> replicas) {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        return spy(new AbstractReadExecutor.NeverSpeculatingReadExecutor(
                                    getReadCommand(keyspace, cf), ConsistencyLevel.ANY, replicas, cfs, 0L));
    }

    public static AbstractReadExecutor getTestSpeculatingReadExecutor(String keyspace, String cf, List<InetAddress> replicas) {
        ColumnFamilyStore cfs = spy(MockSchema.newCFS());
        return spy(new AbstractReadExecutor.SpeculatingReadExecutor(cfs, getReadCommand(keyspace, cf), ConsistencyLevel.ANY, replicas));
    }

    private static ReadCommand getReadCommand(String keyspace, String cf) {
        CellNameType type = Keyspace.open(keyspace).getColumnFamilyStore(cf).getComparator();
        SortedSet<CellName> colList = new TreeSet<CellName>(type);
        colList.add(Util.cellname("col1"));
        DecoratedKey dk = Util.dk("row1");
        return new SliceByNamesReadCommand(keyspace, dk.getKey(), cf, System.currentTimeMillis(), new NamesQueryFilter(colList));
    }
}
