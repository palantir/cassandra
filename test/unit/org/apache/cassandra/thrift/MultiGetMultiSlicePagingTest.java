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

package org.apache.cassandra.thrift;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.assertj.core.api.Assertions.assertThat;

public class MultiGetMultiSlicePagingTest
{
    private static final String KEYSPACE = MultiGetMultiSliceTest.class.getSimpleName();
    private static final String CF_STANDARD = "Standard1";
    private static final ByteBuffer PARTITION_1 = ByteBufferUtil.bytes("Partition1");
    private static final ByteBuffer COLUMN_A = ByteBufferUtil.bytes("a");
    private static final ByteBuffer COLUMN_E = ByteBufferUtil.bytes("e");
    private static final ByteBuffer COLUMN_I = ByteBufferUtil.bytes("i");
    private static final ByteBuffer COLUMN_Y = ByteBufferUtil.bytes("y");
    private static final ByteBuffer COLUMN_Z = ByteBufferUtil.bytes("z");
    private static final KeyPredicate PARTITION_1_RANGE_FROM_A_TO_Z
            = keyPredicateForRange(PARTITION_1, COLUMN_A, COLUMN_Z, 100);
    private static final KeyPredicate PARTITION_1_RANGE_FROM_E_TO_Z
            = keyPredicateForRange(PARTITION_1, COLUMN_E, COLUMN_Z, 100);
    private static final KeyPredicate PARTITION_1_RANGE_FROM_Y
            = keyPredicateForRange(PARTITION_1, COLUMN_Y, EMPTY_BYTE_BUFFER, 100);

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
    public void overlappingRangePredicatesOnSamePartition() throws Exception
    {
        ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);

        List<KeyPredicate> request = ImmutableList.of(PARTITION_1_RANGE_FROM_A_TO_Z);

        Map<ByteBuffer, List<PageResult>> result = server.multiget_multislice_paging(request, cp, ConsistencyLevel.ONE);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(PARTITION_1).size()).isEqualTo(1);
        assertThat(result.get(PARTITION_1).get(0).getColumns().size()).isEqualTo(4);
        assertThat(result.get(PARTITION_1).get(0).getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(PARTITION_1).get(0).getPage_token().column_name).isEqualTo(COLUMN_E);
    }

    @Test
    public void overlappingRangePredicatesOnSamePartitionStartLater() throws Exception
    {
        ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);

        List<KeyPredicate> request = ImmutableList.of(PARTITION_1_RANGE_FROM_E_TO_Z);

        Map<ByteBuffer, List<PageResult>> result = server.multiget_multislice_paging(request, cp, ConsistencyLevel.ONE);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(PARTITION_1).size()).isEqualTo(1);
        assertThat(result.get(PARTITION_1).get(0).getColumns().size()).isEqualTo(4);
        assertThat(result.get(PARTITION_1).get(0).getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(PARTITION_1).get(0).getPage_token().column_name).isEqualTo(COLUMN_I);
    }

    @Test
    public void overlappingRangePredicatesOnSamePartitionEndOfRow() throws Exception
    {
        ColumnParent cp = new ColumnParent(CF_STANDARD);
        addTheAlphabetToRow(PARTITION_1, cp);

        List<KeyPredicate> request = ImmutableList.of(PARTITION_1_RANGE_FROM_Y);

        Map<ByteBuffer, List<PageResult>> result = server.multiget_multislice_paging(request, cp, ConsistencyLevel.ONE);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(PARTITION_1).size()).isEqualTo(1);
        assertThat(result.get(PARTITION_1).get(0).getColumns().size()).isEqualTo(2);
        assertThat(result.get(PARTITION_1).get(0).getPage_token().end_of_row).isEqualTo(true);
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
                    .setValue(new byte[0])
                    .setTimestamp(System.nanoTime());
            server.insert(key, parent, column, ConsistencyLevel.ONE);
        }
    }
}
