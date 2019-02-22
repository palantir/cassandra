/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.thrift;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;

public class MultiGetMultiSliceTest
{
    private static final String KEYSPACE = MultiGetMultiSliceTest.class.getSimpleName();
    private static final String CF_STANDARD = "Standard1";

    private static final ByteBuffer PARTITION_1 = ByteBufferUtil.bytes("Partition1");
    private static final ByteBuffer PARTITION_2 = ByteBufferUtil.bytes("Partition2");
    private static final ByteBuffer COLUMN_A = ByteBufferUtil.bytes("a");
    private static final ByteBuffer COLUMN_B = ByteBufferUtil.bytes("b");
    private static final ByteBuffer COLUMN_C = ByteBufferUtil.bytes("c");

    private static CassandraServer server;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException, IOException, TException
    {
        SchemaLoader.prepareServer();
        new EmbeddedCassandraService().start();
        ThriftSessionManager.instance.setCurrentSocket(new InetSocketAddress(9160));
        SchemaLoader.createKeyspace(KEYSPACE,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARD));
        server = new CassandraServer();
        server.set_keyspace(KEYSPACE);
    }

    @Test
    public void canUseDifferentPredicatesOnDifferentPartitions() throws Exception
    {
        ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);
        addTheAlphabetToRow(PARTITION_2, cp);

        List<KeyPredicate> request = ImmutableList.of(
                new KeyPredicate().setKey(PARTITION_1).setPredicate(slicePredicateForColumns(COLUMN_A)),
                new KeyPredicate().setKey(PARTITION_2).setPredicate(slicePredicateForColumns(COLUMN_B, COLUMN_C)));

        Map<ByteBuffer, List<ColumnOrSuperColumn>> result = server.multiget_multislice(request, cp, ConsistencyLevel.ONE);
        assertColumnNamesMatchPrecisely(ImmutableList.of(COLUMN_A), result.get(PARTITION_1));
        assertColumnNamesMatchPrecisely(ImmutableList.of(COLUMN_B, COLUMN_C), result.get(PARTITION_2));
    }

    @Test
    public void canUseDifferentPredicatesOnTheSamePartition() throws Exception
    {
        ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);

        List<KeyPredicate> request = ImmutableList.of(
                new KeyPredicate().setKey(PARTITION_1).setPredicate(slicePredicateForColumns(COLUMN_A)),
                new KeyPredicate().setKey(PARTITION_1).setPredicate(slicePredicateForColumns(COLUMN_B)));

        Map<ByteBuffer, List<ColumnOrSuperColumn>> result = server.multiget_multislice(request, cp, ConsistencyLevel.ONE);
        assertColumnNamesMatchInAnyOrder(ImmutableList.of(COLUMN_A, COLUMN_B), result.get(PARTITION_1));
    }

    @Test
    public void overlappingPredicatesOnSamePartitionHandled() throws Exception
    {
        ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);

        List<KeyPredicate> request = ImmutableList.of(
        new KeyPredicate().setKey(PARTITION_1).setPredicate(slicePredicateForColumns(COLUMN_A, COLUMN_B)),
        new KeyPredicate().setKey(PARTITION_1).setPredicate(slicePredicateForColumns(COLUMN_B, COLUMN_C)));

        Map<ByteBuffer, List<ColumnOrSuperColumn>> result = server.multiget_multislice(request, cp, ConsistencyLevel.ONE);
        assertColumnNamesMatchInAnyOrder(ImmutableList.of(COLUMN_A, COLUMN_B, COLUMN_C), result.get(PARTITION_1));
    }

    private SlicePredicate slicePredicateForColumns(ByteBuffer... columnNames) {
        return new SlicePredicate()
                .setColumn_names(ImmutableList.copyOf(columnNames));
    }

    private static void addTheAlphabetToRow(ByteBuffer key, ColumnParent parent)
            throws InvalidRequestException, UnavailableException, TimedOutException
    {
        for (char ch = 'a'; ch <= 'z'; ch++)
        {
            Column column = new Column()
                    .setName(ByteBufferUtil.bytes(String.valueOf(ch)))
                    .setValue(new byte [0])
                    .setTimestamp(System.nanoTime());
            server.insert(key, parent, column, ConsistencyLevel.ONE);
        }
    }

    private static void assertColumnNamesMatchPrecisely(List<ByteBuffer> expected, List<ColumnOrSuperColumn> actual)
    {
        Assert.assertEquals(actual + " " + expected + " did not have same number of elements", actual.size(), expected.size());
        for (int i = 0 ; i < expected.size() ; i++)
        {
            Assert.assertEquals(actual.get(i) + " did not equal " + expected.get(i),
                                expected.get(i), actual.get(i).getColumn().bufferForName());
        }
    }

    private static void assertColumnNamesMatchInAnyOrder(List<ByteBuffer> expected, List<ColumnOrSuperColumn> actual)
    {
        Assert.assertEquals(actual + " " + expected + " did not have same number of elements", actual.size(), expected.size());

        List<ByteBuffer> actualBuffers = Lists.newArrayList();
        for (ColumnOrSuperColumn actualColumn : actual) {
            actualBuffers.add(actualColumn.getColumn().bufferForName());
        }
        Collections.sort(actualBuffers);
        Collections.sort(expected);

        for (int i = 0 ; i < expected.size() ; i++)
        {
            Assert.assertEquals(actual.get(i) + " did not equal " + expected.get(i),
                                expected.get(i), actualBuffers.get(i));
        }
    }
}
