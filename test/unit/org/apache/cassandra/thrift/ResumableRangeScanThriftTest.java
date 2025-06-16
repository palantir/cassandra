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

public class ResumableRangeScanThriftTest
{
    public static final String KEYSPACE = "KeyspaceForTest";
    public static final String COLUMN_FAMILY = "ColumnFamilyForTest";

    private static final ByteBuffer PARTITION_1 = ByteBufferUtil.bytes("Partition1");
    private static final ByteBuffer PARTITION_2 = ByteBufferUtil.bytes("Partition2");

    private static final KeyRange KEY_RANGE_1 = new KeyRange().setStart_key(PARTITION_1).setEnd_key(PARTITION_1);
    private static final KeyRange KEY_RANGE_1_TO_2 = new KeyRange().setStart_key(PARTITION_1).setEnd_key(PARTITION_2);

    private static final ByteBuffer COLUMN_A = ByteBufferUtil.bytes("a");
    private static final ByteBuffer COLUMN_E = ByteBufferUtil.bytes("e");
    private static final ByteBuffer COLUMN_I = ByteBufferUtil.bytes("i");
    private static final ByteBuffer COLUMN_Y = ByteBufferUtil.bytes("y");
    private static final ByteBuffer COLUMN_Z = ByteBufferUtil.bytes("z");

    private static final SlicePredicate SLICE_PREDICATE_A_TO_Z = slicePredicateForRange(COLUMN_A, COLUMN_Z, 100);
    private static final SlicePredicate SLICE_PREDICATE_E_TO_Z = slicePredicateForRange(COLUMN_E, COLUMN_Z, 100);
    private static final SlicePredicate SLICE_PREDICATE_FROM_Y = slicePredicateForRange(COLUMN_Y, EMPTY_BYTE_BUFFER, 100);
    private static final SlicePredicate REVERSED_SLICE_PREDICATE =
            new SlicePredicate().setSlice_range(new SliceRange().setStart(EMPTY_BYTE_BUFFER).setFinish(EMPTY_BYTE_BUFFER).setCount(100).setReversed(true));

    private static final KeyPredicate PARTITION_1_RANGE_FROM_A_TO_Z = keyPredicateForSlice(PARTITION_1, SLICE_PREDICATE_A_TO_Z);
    private static final KeyPredicate PARTITION_1_RANGE_FROM_Y = keyPredicateForSlice(PARTITION_1, SLICE_PREDICATE_FROM_Y);
    private static final KeyPredicate PARTITION_2_RANGE_FROM_E_TO_Z = keyPredicateForSlice(PARTITION_2, SLICE_PREDICATE_E_TO_Z);
    private static final KeyPredicate PARTITION_1_REVERSE = keyPredicateForSlice(PARTITION_1, REVERSED_SLICE_PREDICATE);

    private static final String REVERSE_FILTER_VALIDATION_ERROR_MESSAGE = "do not support reversed queries";

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
                SchemaLoader.standardCFMD(KEYSPACE, COLUMN_FAMILY));
        server = new CassandraServer();
        server.set_keyspace(KEYSPACE);
    }

    @Test
    public void multiGetSlicePaging_FullPartition() throws Exception
    {
        ColumnParent cp = new ColumnParent(COLUMN_FAMILY);
        addTheAlphabetToRow(PARTITION_1, cp);

        Map<ByteBuffer, PageResult> result = server.multiget_slice_paging(ImmutableList.of(PARTITION_1), cp, SLICE_PREDICATE_A_TO_Z,
                ConsistencyLevel.ONE, null);

        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(PARTITION_1).getColumns().size()).isEqualTo(4);
        assertThat(result.get(PARTITION_1).getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(PARTITION_1).getPage_token().column_name).isEqualTo(COLUMN_E);
    }

    @Test
    public void multiGetSlicePaging_MultipleFullPartitions() throws Exception
    {
        ColumnParent cp = new ColumnParent(COLUMN_FAMILY);
        addTheAlphabetToRow(PARTITION_1, cp);
        addTheAlphabetToRow(PARTITION_2, cp);

        Map<ByteBuffer, PageResult> result = server.multiget_slice_paging(ImmutableList.of(PARTITION_1, PARTITION_2), cp, SLICE_PREDICATE_A_TO_Z,
                ConsistencyLevel.ONE, null);

        assertThat(result.size()).isEqualTo(2);

        assertThat(result.get(PARTITION_1).getColumns().size()).isEqualTo(4);
        assertThat(result.get(PARTITION_1).getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(PARTITION_1).getPage_token().column_name).isEqualTo(COLUMN_E);

        assertThat(result.get(PARTITION_2).getColumns().size()).isEqualTo(4);
        assertThat(result.get(PARTITION_2).getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(PARTITION_2).getPage_token().column_name).isEqualTo(COLUMN_E);
    }

    @Test
    public void multiGetSlicePaging_MultiplePartialPartitions() throws Exception
    {
        ColumnParent cp = new ColumnParent(COLUMN_FAMILY);
        addTheAlphabetToRow(PARTITION_1, cp);
        addTheAlphabetToRow(PARTITION_2, cp);

        Map<ByteBuffer, PageResult> result = server.multiget_slice_paging(ImmutableList.of(PARTITION_1, PARTITION_2), cp, SLICE_PREDICATE_E_TO_Z,
                ConsistencyLevel.ONE, null);

        assertThat(result.size()).isEqualTo(2);

        assertThat(result.get(PARTITION_1).getColumns().size()).isEqualTo(4);
        assertThat(result.get(PARTITION_1).getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(PARTITION_1).getPage_token().column_name).isEqualTo(COLUMN_I);

        assertThat(result.get(PARTITION_2).getColumns().size()).isEqualTo(4);
        assertThat(result.get(PARTITION_2).getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(PARTITION_2).getPage_token().column_name).isEqualTo(COLUMN_I);
    }

    @Test
    public void multiGetSlicePaging_MultiplePartialPartitionsEndOfRow() throws Exception
    {
        ColumnParent cp = new ColumnParent(COLUMN_FAMILY);
        addTheAlphabetToRow(PARTITION_1, cp);
        addTheAlphabetToRow(PARTITION_2, cp);

        Map<ByteBuffer, PageResult> result = server.multiget_slice_paging(ImmutableList.of(PARTITION_1, PARTITION_2), cp, SLICE_PREDICATE_FROM_Y,
                ConsistencyLevel.ONE, null);

        assertThat(result.size()).isEqualTo(2);

        assertThat(result.get(PARTITION_1).getColumns().size()).isEqualTo(2);
        assertThat(result.get(PARTITION_1).getPage_token().end_of_row).isEqualTo(true);
        assertThat(result.get(PARTITION_1).getPage_token().column_name).isNull();

        assertThat(result.get(PARTITION_2).getColumns().size()).isEqualTo(2);
        assertThat(result.get(PARTITION_2).getPage_token().end_of_row).isEqualTo(true);
        assertThat(result.get(PARTITION_2).getPage_token().column_name).isNull();
    }

    @Test
    public void multiGetSlicePaging_ThrowsIfReversed() throws Exception
    {
        ColumnParent cp = new ColumnParent(COLUMN_FAMILY);

        boolean caughtExpectedError = false;
        try
        {
            server.multiget_slice_paging(ImmutableList.of(PARTITION_1), cp, REVERSED_SLICE_PREDICATE, ConsistencyLevel.ONE, null);
        }
        catch (AssertionError e)
        {
            if (e.getMessage().contains(REVERSE_FILTER_VALIDATION_ERROR_MESSAGE))
            {
                caughtExpectedError = true;
            }
        }
        assertThat(caughtExpectedError).isTrue();
    }


    @Test
    public void multiGetMultiSlicePaging_FullPartition() throws Exception
    {
        ColumnParent cp = new ColumnParent(COLUMN_FAMILY);
        addTheAlphabetToRow(PARTITION_1, cp);

        List<KeyPredicate> request = ImmutableList.of(PARTITION_1_RANGE_FROM_A_TO_Z);

        Map<ByteBuffer, List<PageResult>> result = server.multiget_multislice_paging(request, cp, ConsistencyLevel.ONE, null);

        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(PARTITION_1).size()).isEqualTo(1);
        assertThat(result.get(PARTITION_1).get(0).getColumns().size()).isEqualTo(4);
        assertThat(result.get(PARTITION_1).get(0).getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(PARTITION_1).get(0).getPage_token().column_name).isEqualTo(COLUMN_E);
    }

    @Test
    public void multiGetMultiSlicePaging_FullAndPartialPartitions() throws Exception
    {
        ColumnParent cp = new ColumnParent(COLUMN_FAMILY);
        addTheAlphabetToRow(PARTITION_1, cp);
        addTheAlphabetToRow(PARTITION_2, cp);

        List<KeyPredicate> request = ImmutableList.of(PARTITION_1_RANGE_FROM_A_TO_Z, PARTITION_2_RANGE_FROM_E_TO_Z);

        Map<ByteBuffer, List<PageResult>> result = server.multiget_multislice_paging(request, cp, ConsistencyLevel.ONE, null);

        assertThat(result.size()).isEqualTo(2);

        assertThat(result.get(PARTITION_1).size()).isEqualTo(1);
        assertThat(result.get(PARTITION_1).get(0).getColumns().size()).isEqualTo(4);
        assertThat(result.get(PARTITION_1).get(0).getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(PARTITION_1).get(0).getPage_token().column_name).isEqualTo(COLUMN_E);

        assertThat(result.get(PARTITION_2).size()).isEqualTo(1);
        assertThat(result.get(PARTITION_2).get(0).getColumns().size()).isEqualTo(4);
        assertThat(result.get(PARTITION_2).get(0).getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(PARTITION_2).get(0).getPage_token().column_name).isEqualTo(COLUMN_I);
    }

    @Test
    public void multiGetMultiSlicePaging_FullAndPartialOnSamePartition() throws Exception
    {
        ColumnParent cp = new ColumnParent(COLUMN_FAMILY);
        addTheAlphabetToRow(PARTITION_1, cp);

        List<KeyPredicate> request = ImmutableList.of(PARTITION_1_RANGE_FROM_A_TO_Z, PARTITION_1_RANGE_FROM_Y);

        Map<ByteBuffer, List<PageResult>> result = server.multiget_multislice_paging(request, cp, ConsistencyLevel.ONE, null);

        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(PARTITION_1).size()).isEqualTo(2);

        assertThat(result.get(PARTITION_1).get(0).getColumns().size()).isEqualTo(4);
        assertThat(result.get(PARTITION_1).get(0).getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(PARTITION_1).get(0).getPage_token().column_name).isEqualTo(COLUMN_E);

        assertThat(result.get(PARTITION_1).get(1).getColumns().size()).isEqualTo(2);
        assertThat(result.get(PARTITION_1).get(1).getPage_token().end_of_row).isEqualTo(true);
        assertThat(result.get(PARTITION_1).get(1).getPage_token().column_name).isNull();
    }

    @Test
    public void multiGetMultiSlicePaging_ThrowsIfReversed() throws Exception
    {
        ColumnParent cp = new ColumnParent(COLUMN_FAMILY);
        List<KeyPredicate> request = ImmutableList.of(PARTITION_1_REVERSE);

        boolean caughtExpectedError = false;
        try
        {
            server.multiget_multislice_paging(request, cp, ConsistencyLevel.ONE, null);
        }
        catch (AssertionError e)
        {
            if (e.getMessage().contains(REVERSE_FILTER_VALIDATION_ERROR_MESSAGE))
            {
                caughtExpectedError = true;
            }
        }
        assertThat(caughtExpectedError).isTrue();
    }


    @Test
    public void getRangeSlicesPaging_FullPartition() throws Exception
    {
        ColumnParent cp = new ColumnParent(COLUMN_FAMILY);
        addTheAlphabetToRow(PARTITION_1, cp);

        List<KeySlicePage> result = server.get_range_slices_paging(cp, SLICE_PREDICATE_A_TO_Z, KEY_RANGE_1, ConsistencyLevel.ONE, null);

        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getPageResult().getColumns().size()).isEqualTo(4);
        assertThat(result.get(0).getPageResult().getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(0).getPageResult().getPage_token().column_name).isEqualTo(COLUMN_E);
    }

    @Test
    public void getRangeSlicesPaging_MultipleFullPartitions() throws Exception
    {
        ColumnParent cp = new ColumnParent(COLUMN_FAMILY);
        addTheAlphabetToRow(PARTITION_1, cp);
        addTheAlphabetToRow(PARTITION_2, cp);

        List<KeySlicePage> result = server.get_range_slices_paging(cp, SLICE_PREDICATE_A_TO_Z, KEY_RANGE_1_TO_2, ConsistencyLevel.ONE, null);

        assertThat(result.size()).isEqualTo(2);

        assertThat(result.get(0).getPageResult().getColumns().size()).isEqualTo(4);
        assertThat(result.get(0).getPageResult().getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(0).getPageResult().getPage_token().column_name).isEqualTo(COLUMN_E);

        assertThat(result.get(1).getPageResult().getColumns().size()).isEqualTo(4);
        assertThat(result.get(1).getPageResult().getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(1).getPageResult().getPage_token().column_name).isEqualTo(COLUMN_E);
    }

    @Test
    public void getRangeSlicesPaging_MultiplePartialPartitions() throws Exception
    {
        ColumnParent cp = new ColumnParent(COLUMN_FAMILY);
        addTheAlphabetToRow(PARTITION_1, cp);
        addTheAlphabetToRow(PARTITION_2, cp);

        List<KeySlicePage> result = server.get_range_slices_paging(cp, SLICE_PREDICATE_E_TO_Z, KEY_RANGE_1_TO_2, ConsistencyLevel.ONE, null);

        assertThat(result.size()).isEqualTo(2);

        assertThat(result.get(0).getPageResult().getColumns().size()).isEqualTo(4);
        assertThat(result.get(0).getPageResult().getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(0).getPageResult().getPage_token().column_name).isEqualTo(COLUMN_I);

        assertThat(result.get(1).getPageResult().getColumns().size()).isEqualTo(4);
        assertThat(result.get(1).getPageResult().getPage_token().end_of_row).isEqualTo(false);
        assertThat(result.get(1).getPageResult().getPage_token().column_name).isEqualTo(COLUMN_I);
    }

    @Test
    public void getRangeSlicesPaging_MultiplePartialPartitionsEndOfRow() throws Exception
    {
        ColumnParent cp = new ColumnParent(COLUMN_FAMILY);
        addTheAlphabetToRow(PARTITION_1, cp);
        addTheAlphabetToRow(PARTITION_2, cp);

        List<KeySlicePage> result = server.get_range_slices_paging(cp, SLICE_PREDICATE_FROM_Y, KEY_RANGE_1_TO_2, ConsistencyLevel.ONE, null);

        assertThat(result.size()).isEqualTo(2);

        assertThat(result.get(0).getPageResult().getColumns().size()).isEqualTo(2);
        assertThat(result.get(0).getPageResult().getPage_token().end_of_row).isEqualTo(true);
        assertThat(result.get(0).getPageResult().getPage_token().column_name).isNull();

        assertThat(result.get(1).getPageResult().getColumns().size()).isEqualTo(2);
        assertThat(result.get(1).getPageResult().getPage_token().end_of_row).isEqualTo(true);
        assertThat(result.get(1).getPageResult().getPage_token().column_name).isNull();
    }

    @Test
    public void getRangeSlicesPaging_ThrowsIfReversed() throws Exception
    {
        ColumnParent cp = new ColumnParent(COLUMN_FAMILY);

        boolean caughtExpectedError = false;
        try
        {
            server.get_range_slices_paging(cp, REVERSED_SLICE_PREDICATE, KEY_RANGE_1_TO_2, ConsistencyLevel.ONE, null);
        }
        catch (AssertionError e)
        {
            if (e.getMessage().contains(REVERSE_FILTER_VALIDATION_ERROR_MESSAGE))
            {
                caughtExpectedError = true;
            }
        }
        assertThat(caughtExpectedError).isTrue();
    }

    private static KeyPredicate keyPredicateForSlice(ByteBuffer key, SlicePredicate slicePredicate)
    {
        return new KeyPredicate()
                .setKey(key)
                .setPredicate(slicePredicate);
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
            server.insert(key, parent, column, ConsistencyLevel.ONE, null);
        }
    }
}
