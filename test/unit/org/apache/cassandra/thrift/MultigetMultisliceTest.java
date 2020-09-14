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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import net.jpountz.util.ByteBufferUtils;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.assertj.core.api.ThrowableAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MultigetMultisliceTest
{
    private static final String KEYSPACE = MultigetMultisliceTest.class.getSimpleName();
    private static final String CF_STANDARD = "Standard1";

    private static final ByteBuffer PARTITION_1 = ByteBufferUtil.bytes("Partition1");
    private static final ByteBuffer PARTITION_2 = ByteBufferUtil.bytes("Partition2");
    private static final ByteBuffer COLUMN_A = ByteBufferUtil.bytes("a");
    private static final ByteBuffer COLUMN_B = ByteBufferUtil.bytes("b");
    private static final ByteBuffer COLUMN_C = ByteBufferUtil.bytes("c");
    private static final ByteBuffer COLUMN_D = ByteBufferUtil.bytes("d");
    private static final ByteBuffer COLUMN_Z = ByteBufferUtil.bytes("z");

    private static final KeyPredicate PARTITION_1_COLUMN_A = keyPredicateForColumns(PARTITION_1, COLUMN_A);
    private static final KeyPredicate PARTITION_1_COLUMN_B = keyPredicateForColumns(PARTITION_1, COLUMN_B);
    private static final KeyPredicate PARTITION_1_COLUMNS_AB = keyPredicateForColumns(PARTITION_1, COLUMN_A, COLUMN_B);
    private static final KeyPredicate PARTITION_1_COLUMNS_BC = keyPredicateForColumns(PARTITION_1, COLUMN_B, COLUMN_C);
    private static final KeyPredicate PARTITION_2_COLUMNS_BC = keyPredicateForColumns(PARTITION_2, COLUMN_B, COLUMN_C);

    private static final KeyPredicate PARTITION_1_RANGE_THREE_FROM_A_TO_Z
    = keyPredicateForRange(PARTITION_1, COLUMN_A, COLUMN_Z, 3);
    private static final KeyPredicate PARTITION_1_RANGE_THREE_FROM_B_TO_Z
    = keyPredicateForRange(PARTITION_1, COLUMN_B, COLUMN_Z, 3);

    private static EmbeddedCassandraService cassandra;
    private Cassandra.Client client = null;

    public static void setup() throws IOException
    {
        cassandra = new EmbeddedCassandraService();
        cassandra.start();
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException, IOException
    {
        SchemaLoader.prepareServer();
        setup();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    CFMetaData.Builder.create(KEYSPACE, CF_STANDARD, true, false, false)
                                                      .addPartitionKey("pk", AsciiType.instance)
                                                      .addClusteringColumn("ck", AsciiType.instance)
                                                      .addRegularColumn("val", AsciiType.instance)
                                                      .build());
    }

    @Test
    public void differentPredicatesOnDifferentPartitions() throws Exception
    {
        ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);
        addTheAlphabetToRow(PARTITION_2, cp);

        List<KeyPredicate> request = ImmutableList.of(PARTITION_1_COLUMN_A, PARTITION_2_COLUMNS_BC);

        Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> result = getClient().multiget_multislice(request, cp, ConsistencyLevel.ONE);
        assertColumnNamesMatchPrecisely(ImmutableList.of(COLUMN_A), Iterables.getOnlyElement(result.get(PARTITION_1)));
        assertColumnNamesMatchPrecisely(ImmutableList.of(COLUMN_B, COLUMN_C), Iterables.getOnlyElement(result.get(PARTITION_2)));
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void disjointPredicatesOnSamePartition() throws Exception
    {
        ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);

        List<KeyPredicate> request = ImmutableList.of(PARTITION_1_COLUMN_A, PARTITION_1_COLUMN_B);

        Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> result = getClient().multiget_multislice(request, cp, ConsistencyLevel.ONE);

        assertColumnNameBatchesMatch(ImmutableList.<List<ByteBuffer>>of(ImmutableList.of(COLUMN_A),
                                                                        ImmutableList.of(COLUMN_B)),
                                     result.get(PARTITION_1));
        assertThat(result.size()).isEqualTo(1);
    }

    @Test
    public void disjointRangePredicatesOnSamePartition() throws Exception
    {
        ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);

        KeyPredicate partition1RangeAB = keyPredicateForRange(PARTITION_1, COLUMN_A, COLUMN_B, 100);
        KeyPredicate partition1RangeCD = keyPredicateForRange(PARTITION_1, COLUMN_C, COLUMN_D, 100);
        List<KeyPredicate> request = ImmutableList.of(partition1RangeAB, partition1RangeCD);

        Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> result = getClient().multiget_multislice(request, cp, ConsistencyLevel.ONE);
        assertColumnNameBatchesMatch(ImmutableList.<List<ByteBuffer>>of(ImmutableList.of(COLUMN_A, COLUMN_B),
                                                                        ImmutableList.of(COLUMN_C, COLUMN_D)),
                                     result.get(PARTITION_1));
        assertThat(result.size()).isEqualTo(1);
    }

    @Test
    public void overlappingPredicatesOnSamePartition() throws Exception
    {
        ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);

        List<KeyPredicate> request = ImmutableList.of(PARTITION_1_COLUMNS_AB, PARTITION_1_COLUMNS_BC);

        Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> result = getClient().multiget_multislice(request, cp, ConsistencyLevel.ONE);
        assertColumnNameBatchesMatch(ImmutableList.<List<ByteBuffer>>of(ImmutableList.of(COLUMN_A, COLUMN_B),
                                                                        ImmutableList.of(COLUMN_B, COLUMN_C)),
                                     result.get(PARTITION_1));
        assertThat(result.size()).isEqualTo(1);
    }

    @Test
    public void overlappingPredicatesOnSamePartitionWithRangeThrows() throws Exception
    {
        final ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);

        final List<KeyPredicate> request = ImmutableList.of(PARTITION_1_COLUMN_B, PARTITION_1_RANGE_THREE_FROM_A_TO_Z);

        assertThatThrownBy(new ThrowableAssert.ThrowingCallable()
        {
            public void call() throws Throwable
            {
                getClient().multiget_multislice(request, cp, ConsistencyLevel.ONE);
            }
        }).isInstanceOf(TTransportException.class);
          //.hasMessageContaining("Conflicting thriftify details found between commands");
    }

    @Test
    public void overlappingRangePredicatesOnSamePartition() throws Exception
    {
        ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);

        List<KeyPredicate> request = ImmutableList.of(PARTITION_1_RANGE_THREE_FROM_A_TO_Z,
                                                      PARTITION_1_RANGE_THREE_FROM_B_TO_Z);

        Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> result = getClient().multiget_multislice(request, cp, ConsistencyLevel.ONE);
        assertColumnNameBatchesMatch(ImmutableList.<List<ByteBuffer>>of(ImmutableList.of(COLUMN_B, COLUMN_C, COLUMN_D),
                                                                        ImmutableList.of(COLUMN_A, COLUMN_B, COLUMN_C)),
                                     result.get(PARTITION_1));
        assertThat(result.size()).isEqualTo(1);
    }

    @Test
    public void reverseAndForwardOnRangePredicatesSimultaneouslyThrows() throws Exception
    {
        final ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);

        KeyPredicate partition1RangeThreeFromAToZReversed = new KeyPredicate()
                                                            .setKey(PARTITION_1)
                                                            .setPredicate(new SlicePredicate()
                                                                          .setSlice_range(new SliceRange()
                                                                                          .setStart(COLUMN_Z)
                                                                                          .setFinish(COLUMN_A)
                                                                                          .setCount(3)
                                                                                          .setReversed(true)));
        final List<KeyPredicate> request = ImmutableList.of(PARTITION_1_RANGE_THREE_FROM_A_TO_Z,
                                                            partition1RangeThreeFromAToZReversed);

        assertThatThrownBy(new ThrowableAssert.ThrowingCallable()
        {
            public void call() throws Throwable
            {
                getClient().multiget_multislice(request, cp, ConsistencyLevel.ONE);
            }
        }).isInstanceOf(TTransportException.class);
          //.hasMessageContaining("Conflicting thriftify details found between commands");
    }

    @Test
    public void handlesRequestWithManyPredicatesOnTheSamePartition() throws Exception
    {
        ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);

        List<KeyPredicate> request = Lists.newArrayListWithExpectedSize('z' - 'a' + 1);
        for (char ch = 'a'; ch <= 'z'; ch++) {
            request.add(keyPredicateForColumns(PARTITION_1, ByteBufferUtil.bytes(String.valueOf(ch))));
        }

        Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> result = getClient().multiget_multislice(request, cp, ConsistencyLevel.ONE);
        List<List<ByteBuffer>> expected = Lists.newArrayList();
        for (char ch = 'a'; ch <= 'z'; ch++) {
            List<ByteBuffer> expectedBuffer = Lists.newArrayList(ByteBufferUtil.bytes(String.valueOf(ch)));
            expected.add(expectedBuffer);
        }
        assertColumnNameBatchesMatch(expected, result.get(PARTITION_1));
        Assert.assertEquals(result.size(), 1);
    }

    @Test
    public void handlesRequestWithMultipleIdenticalKeyPredicates() throws Exception
    {
        ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);

        List<KeyPredicate> request = ImmutableList.of(PARTITION_1_COLUMNS_AB, PARTITION_1_COLUMNS_AB);

        Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> result = getClient().multiget_multislice(request, cp, ConsistencyLevel.ONE);
        assertColumnNameBatchesMatch(ImmutableList.<List<ByteBuffer>>of(ImmutableList.of(COLUMN_A, COLUMN_B),
                                                                        ImmutableList.of(COLUMN_A, COLUMN_B)),
                                     result.get(PARTITION_1));
        Assert.assertEquals(result.size(), 1);
    }

    @Test
    public void slicePredicateTest() throws TException
    {
        ColumnParent parent = new ColumnParent(CF_STANDARD);
        ByteBuffer key = ByteBufferUtil.bytes("Partition3");
        for (int idx = 0; idx < 100; idx++)
        {
            Column column = new Column()
                            .setName(ByteBufferUtil.bytes(idx))
                            .setValue(ByteBufferUtil.bytes(UUID.randomUUID().toString()))
                            .setTimestamp(System.nanoTime());
            getClient().insert(key, parent, column, ConsistencyLevel.ONE);
        }
        KeyPredicate keyPredicate = new KeyPredicate().setKey(key).setPredicate(slicePredicateForRange(ByteBufferUtil.bytes(0), ByteBufferUtil.bytes(100), 10));
        Map<ByteBuffer,List<List<ColumnOrSuperColumn>>> data = getClient().multiget_multislice(ImmutableList.of(keyPredicate), parent, ConsistencyLevel.ONE);
        assertThat(data.size()).isEqualTo(10);
    }

    private static KeyPredicate keyPredicateForColumns(ByteBuffer key, ByteBuffer... columnNames)
    {
        return new KeyPredicate()
               .setKey(key)
               .setPredicate(slicePredicateForColumns(columnNames));
    }

    private static SlicePredicate slicePredicateForColumns(ByteBuffer... columnNames)
    {
        return new SlicePredicate()
               .setColumn_names(ImmutableList.copyOf(columnNames));
    }

    private static KeyPredicate keyPredicateForRange(ByteBuffer key, ByteBuffer start, ByteBuffer finish, int count)
    {
        return new KeyPredicate()
               .setKey(key)
               .setPredicate(slicePredicateForRange(start, finish, count));
    }


    private static SlicePredicate slicePredicateForRange(ByteBuffer start, ByteBuffer finish, int count)
    {
        return new SlicePredicate()
               .setSlice_range(new SliceRange().setStart(start).setFinish(finish).setCount(count));
    }

    private void addTheAlphabetToRow(ByteBuffer key, ColumnParent parent) throws TException
    {
        for (char ch = 'a'; ch <= 'z'; ch++)
        {
            Column column = new Column()
                            .setName(ByteBufferUtil.bytes(String.valueOf(ch)))
                            .setValue(new byte [0])
                            .setTimestamp(System.nanoTime());
            getClient().insert(key, parent, column, ConsistencyLevel.ONE);
        }
    }

    private static void assertColumnNamesMatchPrecisely(List<ByteBuffer> expected, List<ColumnOrSuperColumn> actual)
    {
        Assert.assertEquals(expected + " " + actual + " did not have same number of elements", expected.size(), actual.size());
        for (int i = 0 ; i < expected.size() ; i++)
        {
            Assert.assertEquals(actual.get(i) + " did not equal " + expected.get(i),
                                expected.get(i), actual.get(i).getColumn().bufferForName());
        }
    }

    private static void assertColumnNameBatchesMatch(List<List<ByteBuffer>> expected, List<List<ColumnOrSuperColumn>> actual)
    {
        List<List<ByteBuffer>> actualBuffers = new ArrayList<>();
        for (List<ColumnOrSuperColumn> actualBatch : actual) {
            List<ByteBuffer> actualBatchAsBuffers = new ArrayList<>();
            for (ColumnOrSuperColumn columnOrSuperColumn : actualBatch) {
                actualBatchAsBuffers.add(columnOrSuperColumn.getColumn().bufferForName());
            }
            actualBuffers.add(actualBatchAsBuffers);
        }

        assertThat(expected).usingElementComparator(new Comparator<List<ByteBuffer>>()
        {
            @Override
            public int compare(List<ByteBuffer> o1, List<ByteBuffer> o2)
            {
                List<ByteBuffer> o1Copy = new ArrayList<>(o1);
                List<ByteBuffer> o2Copy = new ArrayList<>(o2);
                Collections.sort(o1Copy);
                Collections.sort(o2Copy);
                return Ordering.<ByteBuffer>natural().lexicographical().compare(o1Copy, o2Copy);
            }
        })
        .containsExactlyInAnyOrderElementsOf(actualBuffers);
    }

    /**
     * Gets a connection to the localhost client
     *
     * @return
     * @throws TTransportException
     */
    private Cassandra.Client getClient() throws TException
    {
        TSocket socket = new TSocket("localhost", DatabaseDescriptor.getRpcPort());
        socket.setTimeout(1000 * 60 * 60);
        TTransport tr = new TFramedTransport(socket);
        TProtocol proto = new TBinaryProtocol(tr);
        if (client == null)
        {
            client = new Cassandra.Client(proto);
            tr.open();
            client.set_keyspace(KEYSPACE);
        }
        return client;
    }
}
