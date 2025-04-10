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

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RangeSliceReply;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.PageToken;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.Util.tombstone;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ResumableRangeScanCoordinatorTest
{
    public static final String KEYSPACE = "KeyspaceForTest";
    public static final String COLUMN_FAMILY = "ColumnFamilyForTest";
    private final static int REPLICATION_FACTOR = 3;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(REPLICATION_FACTOR),
                SchemaLoader.standardCFMD(KEYSPACE, COLUMN_FAMILY));
    }

    @Test
    public void testSingleMessageWithPageToken_RowDigestResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf.addColumn(column("c1", "v1", 0));
        cf.setPageToken(PageToken.createPageToken(column("c2", "v1", 0)));
        Row row = new Row(key, cf);

        testReadResponses(new RowDigestResolver(KEYSPACE, key, REPLICATION_FACTOR), row, makeReadResponse("127.0.0.1", row));
    }

    @Test
    public void testMultipleMessagesWithPageToken_RowDigestResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf.addColumn(column("c1", "v1", 0));
        cf.setPageToken(PageToken.createPageToken(column("c2", "v1", 0)));
        Row row = new Row(key, cf);

        testReadResponses(new RowDigestResolver(KEYSPACE, key, REPLICATION_FACTOR),
                row,
                makeReadResponse("127.0.0.1", row),
                makeReadResponse("127.0.0.2", row),
                makeReadResponse("127.0.0.3", row));
    }

    @Test
    public void testSingleMessageWithPageToken_RowDataResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf.addColumn(column("c1", "v1", 0));
        cf.setPageToken(PageToken.createPageToken(column("c2", "v1", 0)));
        Row row = new Row(key, cf);

        testReadResponses(new RowDataResolver(KEYSPACE,
                        key,
                        new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 10),
                        System.currentTimeMillis(),
                        REPLICATION_FACTOR),
                row,
                makeReadResponse("127.0.0.1", row));
    }

    @Test
    public void testMultipleMessagesWithPageToken_RowDataResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf.addColumn(column("c1", "v1", 0));
        cf.setPageToken(PageToken.createPageToken(column("c2", "v1", 0)));
        Row row = new Row(key, cf);

        testReadResponses(new RowDataResolver(KEYSPACE,
                        key,
                        new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 10),
                        System.currentTimeMillis(),
                        REPLICATION_FACTOR),
                row,
                makeReadResponse("127.0.0.1", row),
                makeReadResponse("127.0.0.2", row),
                makeReadResponse("127.0.0.3", row));
    }

    @Test
    public void testMultipleMessagesWithDifferentPageTokens_RowDataResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");

        ColumnFamily cf1 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf1.addColumn(column("c1", "v1", 0));
        cf1.setPageToken(PageToken.createPageToken(column("c2", "v2", 0)));

        ColumnFamily cf2 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf2.addColumn(column("c1", "v1", 0));
        cf2.setPageToken(PageToken.createPageToken(column("c3", "v4", 0)));

        ColumnFamily cf3 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf3.addColumn(column("c1", "v1", 0));
        cf3.setPageToken(PageToken.createPageToken(column("c4", "v4", 0)));

        Row row1 = new Row(key, cf1);
        Row row2 = new Row(key, cf2);
        Row row3 = new Row(key, cf3);

        testReadResponses(new RowDataResolver(KEYSPACE,
                        key,
                        new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 10),
                        System.currentTimeMillis(),
                        REPLICATION_FACTOR),
                row1,
                makeReadResponse("127.0.0.1", row1),
                makeReadResponse("127.0.0.2", row2),
                makeReadResponse("127.0.0.3", row3));
    }

    @Test
    public void testMultipleThreadsWithPageTokens_RowDataResolver() throws DigestMismatchException, UnknownHostException, InterruptedException,
            ExecutionException
    {
        ByteBuffer key = bytes("key");

        ColumnFamily cf1 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf1.addColumn(column("c1", "v1", 0));
        cf1.setPageToken(PageToken.createPageToken(column("c2", "v2", 0)));

        ColumnFamily cf2 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf2.addColumn(column("c1", "v1", 0));
        cf2.setPageToken(PageToken.createPageToken(column("c3", "v4", 0)));

        ColumnFamily cf3 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf3.addColumn(column("c1", "v1", 0));
        cf3.setPageToken(PageToken.createPageToken(column("c4", "v4", 0)));

        Row row1 = new Row(key, cf1);
        Row row2 = new Row(key, cf2);
        Row row3 = new Row(key, cf3);

        testReadResponsesMT(new RowDataResolver(KEYSPACE,
                        key,
                        new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 10),
                        System.currentTimeMillis(),
                        REPLICATION_FACTOR),
                row1,
                makeReadResponse("127.0.0.1", row1),
                makeReadResponse("127.0.0.2", row2),
                makeReadResponse("127.0.0.3", row3));
    }

    @Test
    public void testMultipleThreadsWithDifferentCfs_RowDataResolver() throws DigestMismatchException, UnknownHostException, InterruptedException,
            ExecutionException
    {
        ByteBuffer key = bytes("key");

        ColumnFamily cf1 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf1.addColumn(column("c1", "v1", 0));
        cf1.setPageToken(PageToken.createPageToken(column("c2", "v2", 0)));

        ColumnFamily cf2 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf2.addColumn(column("c11", "v11", 0));
        cf2.setPageToken(PageToken.createPageToken(column("c3", "v3", 0)));

        ColumnFamily cf3 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf3.addColumn(column("c111", "v111", 0));
        cf3.setPageToken(PageToken.createPageToken(column("c4", "v4", 0)));

        ColumnFamily resolvedCf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        resolvedCf.addColumn(column("c1", "v1", 0));
        resolvedCf.addColumn(column("c11", "v11", 0));
        resolvedCf.addColumn(column("c111", "v111", 0));
        resolvedCf.setPageToken(PageToken.createPageToken(column("c2", "v2", 0)));

        Row row1 = new Row(key, cf1);
        Row row2 = new Row(key, cf2);
        Row row3 = new Row(key, cf3);
        Row resolved = new Row(key, resolvedCf);

        testReadResponsesMT(new RowDataResolver(KEYSPACE,
                        key,
                        new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 10),
                        System.currentTimeMillis(),
                        REPLICATION_FACTOR),
                resolved,
                makeReadResponse("127.0.0.1", row1),
                makeReadResponse("127.0.0.2", row2),
                makeReadResponse("127.0.0.3", row3));
    }

    @Test
    public void testMultipleThreadsWithDifferentCfsTombstones_RowDataResolver() throws DigestMismatchException, UnknownHostException, InterruptedException,
            ExecutionException
    {
        ByteBuffer key = bytes("key");

        ColumnFamily cf1 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf1.addColumn(column("c1", "v1", 0));
        cf1.setPageToken(PageToken.createPageToken(column("c2", "v2", 0)));

        ColumnFamily cf2 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf2.addColumn(column("c11", "v11", 0));
        cf2.delete(tombstone("c111", "c2", 0, 0)); // tombstone
        cf2.setPageToken(PageToken.createPageToken(column("c3", "v3", 0)));

        ColumnFamily cf3 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf3.addColumn(column("c111", "v111", 0));
        cf3.setPageToken(PageToken.createPageToken(column("c4", "v4", 0)));

        ColumnFamily resolvedCf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        resolvedCf.addColumn(column("c1", "v1", 0));
        resolvedCf.addColumn(column("c11", "v11", 0));
        resolvedCf.delete(tombstone("c111", "c2", 0, 0));
        resolvedCf.setPageToken(PageToken.createPageToken(column("c2", "v2", 0)));

        Row row1 = new Row(key, cf1);
        Row row2 = new Row(key, cf2);
        Row row3 = new Row(key, cf3);
        Row resolved = new Row(key, resolvedCf);

        testReadResponsesMT(new RowDataResolver(KEYSPACE,
                        key,
                        new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 10),
                        System.currentTimeMillis(),
                        REPLICATION_FACTOR),
                resolved,
                makeReadResponse("127.0.0.1", row1),
                makeReadResponse("127.0.0.2", row2),
                makeReadResponse("127.0.0.3", row3));
    }

    @Test
    public void testMultipleThreadsWithEndedPageTokens_RowDataResolver() throws DigestMismatchException, UnknownHostException, InterruptedException,
            ExecutionException
    {
        ByteBuffer key = bytes("key");

        ColumnFamily cf1 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf1.addColumn(column("c1", "v1", 0));
        cf1.setPageToken(PageToken.createPageTokenReachedEnd());

        ColumnFamily cf2 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf2.addColumn(column("c11", "v11", 0));
        cf2.delete(tombstone("c111", "c2", 0, 0)); // tombstone
        cf2.setPageToken(PageToken.createPageTokenReachedEnd());

        ColumnFamily cf3 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf3.addColumn(column("c111", "v111", 0));
        cf3.setPageToken(PageToken.createPageTokenReachedEnd());

        ColumnFamily resolvedCf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        resolvedCf.addColumn(column("c1", "v1", 0));
        resolvedCf.addColumn(column("c11", "v11", 0));
        resolvedCf.delete(tombstone("c111", "c2", 0, 0));
        resolvedCf.setPageToken(PageToken.createPageTokenReachedEnd());

        Row row1 = new Row(key, cf1);
        Row row2 = new Row(key, cf2);
        Row row3 = new Row(key, cf3);
        Row resolved = new Row(key, resolvedCf);

        testReadResponsesMT(new RowDataResolver(KEYSPACE,
                        key,
                        new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 10),
                        System.currentTimeMillis(),
                        REPLICATION_FACTOR),
                resolved,
                makeReadResponse("127.0.0.1", row1),
                makeReadResponse("127.0.0.2", row2),
                makeReadResponse("127.0.0.3", row3));
    }

    @Test
    public void testMultipleThreadsWithMixedPageTokens_RowDataResolver() throws DigestMismatchException, UnknownHostException, InterruptedException,
            ExecutionException
    {
        ByteBuffer key = bytes("key");

        ColumnFamily cf1 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf1.addColumn(column("c1", "v1", 0));
        cf1.setPageToken(PageToken.createPageTokenReachedEnd());

        ColumnFamily cf2 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf2.addColumn(column("c11", "v11", 0));
        cf2.delete(tombstone("c111", "c2", 0, 0)); // tombstone
        cf2.setPageToken(PageToken.createPageToken(column("c3", "v3", 0)));

        ColumnFamily cf3 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf3.addColumn(column("c111", "v111", 0));
        cf3.setPageToken(PageToken.createPageToken(column("c4", "v4", 0)));

        ColumnFamily resolvedCf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        resolvedCf.addColumn(column("c1", "v1", 0));
        resolvedCf.addColumn(column("c11", "v11", 0));
        resolvedCf.delete(tombstone("c111", "c2", 0, 0));
        resolvedCf.setPageToken(PageToken.createPageToken(column("c3", "v3", 0)));

        Row row1 = new Row(key, cf1);
        Row row2 = new Row(key, cf2);
        Row row3 = new Row(key, cf3);
        Row resolved = new Row(key, resolvedCf);

        testReadResponsesMT(new RowDataResolver(KEYSPACE,
                        key,
                        new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 10),
                        System.currentTimeMillis(),
                        REPLICATION_FACTOR),
                resolved,
                makeReadResponse("127.0.0.1", row1),
                makeReadResponse("127.0.0.2", row2),
                makeReadResponse("127.0.0.3", row3));
    }

    @Test
    public void testMultipleThreadsWithMixedPageTokensEnded_RowDataResolver() throws DigestMismatchException, UnknownHostException, InterruptedException,
            ExecutionException
    {
        ByteBuffer key = bytes("key");

        ColumnFamily cf1 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf1.addColumn(column("c1", "v1", 0));
        cf1.setPageToken(PageToken.createPageTokenReachedEnd());

        ColumnFamily cf2 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf2.addColumn(column("c11", "v11", 0));
        cf2.delete(tombstone("c111", "c2", 0, 0)); // tombstone
        cf2.setPageToken(PageToken.createPageToken(column("c3", "v3", 0)));

        ColumnFamily cf3 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf3.addColumn(column("c111", "v111", 0));
        cf3.setPageToken(PageToken.createPageTokenReachedEnd());

        ColumnFamily resolvedCf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        resolvedCf.addColumn(column("c1", "v1", 0));
        resolvedCf.addColumn(column("c11", "v11", 0));
        resolvedCf.delete(tombstone("c111", "c2", 0, 0));
        resolvedCf.setPageToken(PageToken.createPageToken(column("c3", "v3", 0)));

        Row row1 = new Row(key, cf1);
        Row row2 = new Row(key, cf2);
        Row row3 = new Row(key, cf3);
        Row resolved = new Row(key, resolvedCf);

        testReadResponsesMT(new RowDataResolver(KEYSPACE,
                        key,
                        new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 10),
                        System.currentTimeMillis(),
                        REPLICATION_FACTOR),
                resolved,
                makeReadResponse("127.0.0.1", row1),
                makeReadResponse("127.0.0.2", row2),
                makeReadResponse("127.0.0.3", row3));
    }

    private void testReadResponses(AbstractRowResolver resolver, Row expected, MessageIn<ReadResponse>... messages) throws DigestMismatchException
    {
        for (MessageIn<ReadResponse> message : messages)
        {
            resolver.preprocess(message);

            Row row = resolver.getData();
            if (resolver.replies.size() == 1)
            {
                checkSame(expected, row);
            }

            row = resolver.resolve();
            checkSame(expected, row);
        }
    }

    private void testReadResponsesMT(final AbstractRowResolver resolver,
                                     final Row expected,
                                     final MessageIn<ReadResponse>... messages) throws InterruptedException, ExecutionException
    {
        for (MessageIn<ReadResponse> message : messages)
        {
            resolver.preprocess(message);
        }

        final int threadCount = 45;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch finished = new CountDownLatch(threadCount);
        Future<?>[] futures = new Future[threadCount];

        for (int i = 0; i < threadCount; i++)
        {
            futures[i] = executorService.submit(new Runnable()
            {
                public void run()
                {
                    try
                    {
                        Row row = resolver.getData();
                        if (resolver.replies.size() == 1)
                        {
                            checkSame(expected, row);
                        }

                        row = resolver.resolve();
                        checkSame(expected, row);
                    }
                    catch (DigestMismatchException ex)
                    {
                        fail(ex.getMessage());
                    }
                    finally
                    {
                        finished.countDown();
                    }
                }
            });
        }

        finished.await();
        assertEquals(0, executorService.shutdownNow().size());

        for (int i = 0; i < threadCount; i++)
        {
            futures[i].get();
        }

    }

    private MessageIn<ReadResponse> makeReadResponse(String address, Row row) throws UnknownHostException
    {
        return MessageIn.create(InetAddress.getByName(address),
                new ReadResponse(row),
                Collections.<String, byte[]>emptyMap(),
                MessagingService.Verb.INTERNAL_RESPONSE,
                MessagingService.current_version);
    }

    private MessageIn<RangeSliceReply> makeRangeSlice(String address, Row... rows) throws UnknownHostException
    {
        return MessageIn.create(InetAddress.getByName(address),
                new RangeSliceReply(Arrays.asList(rows)),
                Collections.<String, byte[]>emptyMap(),
                MessagingService.Verb.INTERNAL_RESPONSE,
                MessagingService.current_version);
    }

    private void checkSame(Row r1, Row r2)
    {
        assertEquals(r1.key, r2.key);
        assertEquals(r1.cf, r2.cf);
    }
}
