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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;

public class SimplePerformanceTest
{
    private static final String KEYSPACE = SimplePerformanceTest.class.getSimpleName();
    private static final String CF_STANDARD = "Standard1";

    private static final ByteBuffer PARTITION_1 = ByteBufferUtil.bytes("Partition1");
    private static final ByteBuffer COLUMN_A = ByteBufferUtil.bytes("a");
    private static final ByteBuffer COLUMN_B = ByteBufferUtil.bytes("b");
    private static final ByteBuffer COLUMN_C = ByteBufferUtil.bytes("c");

    private static final KeyPredicate PARTITION_1_RANGE_FOUR_FROM_A_TO_C
    = keyPredicateForRange(PARTITION_1, COLUMN_A, COLUMN_C, Integer.MAX_VALUE);

    private static CassandraServer server;
    private static ColumnParent cp;

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
        cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);
        addLotsOfTombstones(PARTITION_1, 'a');
        Keyspace.open(KEYSPACE).flush().forEach(Futures::getUnchecked);
    }

    @Test
    public void testTombstoneHandling()
    {
        ColumnParent cp = new ColumnParent(CF_STANDARD);
        List<KeyPredicate> request = ImmutableList.of(PARTITION_1_RANGE_FOUR_FROM_A_TO_C);

        Duration min = Duration.ofDays(1);
        for (int i = 0; i < 10; i++) {
            Instant before = Instant.now();
            try {
                Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> result = server.multiget_multislice(request, cp, ConsistencyLevel.ONE);
                assertColumnNamesMatchPrecisely(ImmutableList.of(COLUMN_A, COLUMN_B, COLUMN_C), Iterables.getOnlyElement(result.get(PARTITION_1)));
                Duration duration = Duration.between(before, Instant.now());
                min = Ordering.natural().min(min, duration);
                System.out.println(min);
            } catch (Exception e) {
                continue;
            }
        }
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

    private static void addLotsOfTombstones(ByteBuffer key, char prefix) throws TimedOutException, UnavailableException, InvalidRequestException
    {
        for (int j = 0; j < 20; j++) {
            List<Mutation> mutations = new ArrayList<>();
            for (int i = 0; i < 50_000; i++) {
                ByteBuffer from = ByteBuffer.allocate(9);
                from.put(ByteBufferUtil.bytes(String.valueOf(prefix)));
                from.putInt(j * 50_000 + i * 2);
                from.clear();
                ByteBuffer to = ByteBuffer.allocate(9);
                to.put(ByteBufferUtil.bytes(String.valueOf(prefix)));
                to.putInt(j * 50_000 + i * 2 + 1);
                to.clear();
                Mutation mutation = new Mutation();
                SlicePredicate slicePredicate = new SlicePredicate();
                SliceRange range = new SliceRange();
                range.setStart(from);
                range.setFinish(to);
                slicePredicate.setSlice_range(range);
                Deletion deletion = new Deletion();
                deletion.setPredicate(slicePredicate);
                deletion.setTimestamp(System.currentTimeMillis());
                mutation.setDeletion(deletion);
                mutations.add(mutation);
            }
            server.batch_mutate(ImmutableMap.of(key, ImmutableMap.of(CF_STANDARD, mutations)), ConsistencyLevel.ALL);
            Keyspace.open(KEYSPACE).flush().forEach(Futures::getUnchecked);
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
}
