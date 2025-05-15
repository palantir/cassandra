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

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RangeSliceReply;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.PageToken;
import org.apache.cassandra.db.filter.PageTokenDigest;
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
import java.util.Iterator;
import java.util.List;

import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.Util.tombstone;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

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
                makeDigestResponse("127.0.0.2", ColumnFamily.digest(row.cf), row.cf.pageToken().digest()),
                makeDigestResponse("127.0.0.3", ColumnFamily.digest(row.cf), row.cf.pageToken().digest()));
    }

    @Test(expected = DigestMismatchException.class)
    public void testMultipleMessagesWithAndWithoutPageToken_RowDigestResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf.addColumn(column("c1", "v1", 0));
        cf.setPageToken(PageToken.createPageToken(column("c2", "v1", 0)));
        Row row = new Row(key, cf);

        testReadResponses(new RowDigestResolver(KEYSPACE, key, REPLICATION_FACTOR),
                          row,
                          makeReadResponse("127.0.0.1", row),
                          makeDigestResponse("127.0.0.2", ColumnFamily.digest(row.cf), row.cf.pageToken().digest()),
                          makeDigestResponse("127.0.0.3", ColumnFamily.digest(row.cf), null));
    }

    @Test(expected = DigestMismatchException.class)
    public void testMultipleMessagesWithDifferentColumnsSamePageTokens_RowDigestResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf.addColumn(column("c1", "v1", 0));
        cf.setPageToken(PageToken.createPageToken(column("c2", "v1", 0)));
        Row row = new Row(key, cf);

        testReadResponses(new RowDigestResolver(KEYSPACE, key, REPLICATION_FACTOR),
                row,
                makeReadResponse("127.0.0.1", row),
                makeDigestResponse("127.0.0.2", ColumnFamily.digest(null), row.cf.pageToken().digest()),
                makeDigestResponse("127.0.0.3", ColumnFamily.digest(row.cf), row.cf.pageToken().digest()));
    }

    @Test(expected = DigestMismatchException.class)
    public void testMultipleMessagesWithSameColumnsDifferentPageTokens_RowDigestResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf.addColumn(column("c1", "v1", 0));
        cf.setPageToken(PageToken.createPageToken(column("c2", "v1", 0)));
        Row row = new Row(key, cf);

        testReadResponses(new RowDigestResolver(KEYSPACE, key, REPLICATION_FACTOR),
                row,
                makeReadResponse("127.0.0.1", row),
                makeDigestResponse("127.0.0.2", ColumnFamily.digest(row.cf), row.cf.pageToken().digest()),
                makeDigestResponse("127.0.0.3", ColumnFamily.digest(row.cf), PageTokenDigest.createPageTokenReachedEnd()));
    }

    @Test
    public void testMultipleEmptyMessagesWithPageToken_RowDigestResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf.setPageToken(PageToken.createPageToken(column("c2", "v1", 0)));
        Row row = new Row(key, cf);

        assertFalse(cf.hasColumns());
        assertFalse(cf.isMarkedForDelete());
        testReadResponses(new RowDigestResolver(KEYSPACE, key, REPLICATION_FACTOR),
                row,
                makeReadResponse("127.0.0.1", row),
                makeDigestResponse("127.0.0.2", ColumnFamily.digest(row.cf), row.cf.pageToken().digest()),
                makeDigestResponse("127.0.0.3", ColumnFamily.digest(row.cf), row.cf.pageToken().digest()));
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
    public void testMultipleMessagesWithAndWithoutPageToken_RowDataResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");

        ColumnFamily cf1 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf1.addColumn(column("c1", "v1", 0));
        cf1.setPageToken(PageToken.createPageToken(column("c2", "v2", 0)));

        ColumnFamily cf2 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf2.addColumn(column("c1", "v1", 0));

        Row row1 = new Row(key, cf1);
        Row row2 = new Row(key, cf2);

        testReadResponses(new RowDataResolver(KEYSPACE,
                                              key,
                                              new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 10),
                                              System.currentTimeMillis(),
                                              REPLICATION_FACTOR),
                          row1,
                          makeReadResponse("127.0.0.1", row1),
                          makeReadResponse("127.0.0.2", row2));
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
    public void testMultipleMessagesWithDifferentColumnsDifferentPageTokens_RowDataResolver() throws DigestMismatchException, UnknownHostException
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

        testReadResponses(new RowDataResolver(KEYSPACE,
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
    public void testMultipleMessagesWithEndedPageTokens_RowDataResolver() throws DigestMismatchException, UnknownHostException
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

        testReadResponses(new RowDataResolver(KEYSPACE,
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
    public void testMultipleMessagesWithMixedPageTokens_RowDataResolver() throws DigestMismatchException, UnknownHostException
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

        testReadResponses(new RowDataResolver(KEYSPACE,
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
    public void testMultipleMessagesWithDifferentCfsTombstones_RowDataResolver() throws DigestMismatchException, UnknownHostException
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

        testReadResponses(new RowDataResolver(KEYSPACE,
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
    public void testMultipleEmptyMessagesWithPageToken_RowDataResolver() throws DigestMismatchException, UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf.setPageToken(PageToken.createPageToken(column("c2", "v1", 0)));
        Row row = new Row(key, cf);

        assertFalse(cf.hasColumns());
        assertFalse(cf.isMarkedForDelete());
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
    public void testSingleMessageWithPageToken_RangeSliceResponseResolver() throws UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf.addColumn(column("c1", "v1", 0));
        cf.setPageToken(PageToken.createPageToken(column("c2", "v1", 0)));

        Row[] expected = new Row[]{new Row(key, cf)};
        MessageIn<RangeSliceReply> message = makeRangeSlice("127.0.0.1", expected);

        RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(KEYSPACE, System.currentTimeMillis());
        resolver.setSources(Collections.singletonList(message.from));

        testRangeSlices(resolver, expected, message);
    }

    @Test
    public void testMultipleMessagesWithPageToken_RangeSliceResolver() throws UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf.addColumn(column("c1", "v1", 0));
        cf.setPageToken(PageToken.createPageToken(column("c2", "v1", 0)));

        Row[] expected = new Row[]{new Row(key, cf)};

        List<InetAddress> sources = ImmutableList.of(
                InetAddress.getByName("127.0.0.1"),
                InetAddress.getByName("127.0.0.2"),
                InetAddress.getByName("127.0.0.3")
        );

        RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(KEYSPACE, System.currentTimeMillis());
        resolver.setSources(sources);

        testRangeSlices(resolver,
                expected,
                makeRangeSlice("127.0.0.1", expected),
                makeRangeSlice("127.0.0.2", expected),
                makeRangeSlice("127.0.0.3", expected));
    }

    @Test
    public void testMultipleMessagesWithAndWithoutPageToken_RangeSliceResolver() throws UnknownHostException
    {
        ByteBuffer key = bytes("key");

        ColumnFamily cf1 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf1.addColumn(column("c1", "v1", 0));
        cf1.setPageToken(PageToken.createPageToken(column("c2", "v2", 0)));

        ColumnFamily cf2 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf2.addColumn(column("c1", "v1", 0));

        Row row1 = new Row(key, cf1);
        Row row2 = new Row(key, cf2);

        Row[] expected = new Row[]{ row1 };

        List<InetAddress> sources = ImmutableList.of(
        InetAddress.getByName("127.0.0.1"),
        InetAddress.getByName("127.0.0.2")
        );

        RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(KEYSPACE, System.currentTimeMillis());
        resolver.setSources(sources);

        testRangeSlices(resolver,
                        expected,
                        makeRangeSlice("127.0.0.1", row1),
                        makeRangeSlice("127.0.0.2", row2));
    }

    @Test
    public void testMultipleMessagesWithDifferentPageTokens_RangeSliceResolver() throws UnknownHostException
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

        Row[] expected = new Row[]{row1};

        List<InetAddress> sources = ImmutableList.of(
                InetAddress.getByName("127.0.0.1"),
                InetAddress.getByName("127.0.0.2"),
                InetAddress.getByName("127.0.0.3")
        );

        RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(KEYSPACE, System.currentTimeMillis());
        resolver.setSources(sources);

        testRangeSlices(resolver,
                expected,
                makeRangeSlice("127.0.0.1", row1),
                makeRangeSlice("127.0.0.2", row2),
                makeRangeSlice("127.0.0.3", row3));
    }

    @Test
    public void testMultipleMessagesWithDifferentColumnsDifferentPageTokens_RangeSliceResolver() throws UnknownHostException
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

        Row[] expected = new Row[]{resolved};

        List<InetAddress> sources = ImmutableList.of(
                InetAddress.getByName("127.0.0.1"),
                InetAddress.getByName("127.0.0.2"),
                InetAddress.getByName("127.0.0.3")
        );

        RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(KEYSPACE, System.currentTimeMillis());
        resolver.setSources(sources);

        testRangeSlices(resolver,
                expected,
                makeRangeSlice("127.0.0.1", row1),
                makeRangeSlice("127.0.0.2", row2),
                makeRangeSlice("127.0.0.3", row3));
    }

    @Test
    public void testMultipleMessagesWithEndedPageTokens_RangeSliceResolver() throws UnknownHostException
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

        Row[] expected = new Row[]{resolved};

        List<InetAddress> sources = ImmutableList.of(
                InetAddress.getByName("127.0.0.1"),
                InetAddress.getByName("127.0.0.2"),
                InetAddress.getByName("127.0.0.3")
        );

        RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(KEYSPACE, System.currentTimeMillis());
        resolver.setSources(sources);

        testRangeSlices(resolver,
                expected,
                makeRangeSlice("127.0.0.1", row1),
                makeRangeSlice("127.0.0.2", row2),
                makeRangeSlice("127.0.0.3", row3));
    }

    @Test
    public void testMultipleMessagesWithMixedPageTokens_RangeSliceResolver() throws UnknownHostException
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

        Row[] expected = new Row[]{resolved};

        List<InetAddress> sources = ImmutableList.of(
                InetAddress.getByName("127.0.0.1"),
                InetAddress.getByName("127.0.0.2"),
                InetAddress.getByName("127.0.0.3")
        );

        RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(KEYSPACE, System.currentTimeMillis());
        resolver.setSources(sources);

        testRangeSlices(resolver,
                expected,
                makeRangeSlice("127.0.0.1", row1),
                makeRangeSlice("127.0.0.2", row2),
                makeRangeSlice("127.0.0.3", row3));
    }

    @Test
    public void testMultipleMessagesWithDifferentCfsTombstones_RangeSliceResolver() throws UnknownHostException
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

        Row[] expected = new Row[]{resolved};

        List<InetAddress> sources = ImmutableList.of(
                InetAddress.getByName("127.0.0.1"),
                InetAddress.getByName("127.0.0.2"),
                InetAddress.getByName("127.0.0.3")
        );

        RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(KEYSPACE, System.currentTimeMillis());
        resolver.setSources(sources);

        testRangeSlices(resolver,
                expected,
                makeRangeSlice("127.0.0.1", row1),
                makeRangeSlice("127.0.0.2", row2),
                makeRangeSlice("127.0.0.3", row3));
    }

    @Test
    public void testMultipleKeys_RangeSliceResolver() throws UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ByteBuffer key2 = bytes("key2");

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf.addColumn(column("c1", "v1", 0));
        cf.setPageToken(PageToken.createPageToken(column("c2", "v2", 0)));

        ColumnFamily cf2 = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf2.addColumn(column("c2", "v2", 0));
        cf2.setPageToken(PageToken.createPageToken(column("c3", "v3", 0)));

        Row row1 = new Row(key, cf);
        Row row2 = new Row(key2, cf2);

        Row[] expected = new Row[]{row1, row2};

        List<InetAddress> sources = ImmutableList.of(
                InetAddress.getByName("127.0.0.1"),
                InetAddress.getByName("127.0.0.2"),
                InetAddress.getByName("127.0.0.3")
        );

        RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(KEYSPACE, System.currentTimeMillis());
        resolver.setSources(sources);

        testRangeSlices(resolver,
                expected,
                makeRangeSlice("127.0.0.1", row1, row2),
                makeRangeSlice("127.0.0.2", row1, row2),
                makeRangeSlice("127.0.0.3", row1, row2));
    }

    @Test
    public void testMultipleEmptyMessagesWithPageToken_RangeSliceResolver() throws UnknownHostException
    {
        ByteBuffer key = bytes("key");
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COLUMN_FAMILY);
        cf.setPageToken(PageToken.createPageToken(column("c2", "v1", 0)));
        Row row = new Row(key, cf);

        Row[] expected = new Row[]{row};

        List<InetAddress> sources = ImmutableList.of(
                InetAddress.getByName("127.0.0.1"),
                InetAddress.getByName("127.0.0.2"),
                InetAddress.getByName("127.0.0.3")
        );

        RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(KEYSPACE, System.currentTimeMillis());
        resolver.setSources(sources);

        assertFalse(cf.hasColumns());
        assertFalse(cf.isMarkedForDelete());
        testRangeSlices(resolver,
                expected,
                makeRangeSlice("127.0.0.1", row),
                makeRangeSlice("127.0.0.2", row),
                makeRangeSlice("127.0.0.3", row));
    }

    private void testReadResponses(AbstractRowResolver resolver, Row expected, MessageIn<ReadResponse>... messages) throws DigestMismatchException
    {
        for (MessageIn<ReadResponse> message : messages)
        {
            resolver.preprocess(message);
        }
        checkSame(expected, resolver.resolve());
    }

    private void testRangeSlices(RangeSliceResponseResolver resolver, Row[] expected, MessageIn<RangeSliceReply>... messages)
    {
        for (MessageIn<RangeSliceReply> message : messages)
        {
            resolver.preprocess(message);
        }
        Iterator<Row> rowIt = resolver.resolve().iterator();
        assertNotNull(rowIt);

        for (Row r : expected)
        {
            checkSame(r, rowIt.next());
        }
    }

    private MessageIn<ReadResponse> makeDigestResponse(String address, ByteBuffer digest, PageTokenDigest pageTokenDigest) throws UnknownHostException
    {
        return MessageIn.create(InetAddress.getByName(address),
                new ReadResponse(digest, pageTokenDigest),
                Collections.emptyMap(),
                MessagingService.Verb.INTERNAL_RESPONSE,
                MessagingService.current_version);
    }

    private MessageIn<ReadResponse> makeReadResponse(String address, Row row) throws UnknownHostException
    {
        return MessageIn.create(InetAddress.getByName(address),
                new ReadResponse(row),
                Collections.emptyMap(),
                MessagingService.Verb.INTERNAL_RESPONSE,
                MessagingService.current_version);
    }

    private MessageIn<RangeSliceReply> makeRangeSlice(String address, Row... rows) throws UnknownHostException
    {
        return MessageIn.create(InetAddress.getByName(address),
                new RangeSliceReply(Arrays.asList(rows)),
                Collections.emptyMap(),
                MessagingService.Verb.INTERNAL_RESPONSE,
                MessagingService.current_version);
    }

    private void checkSame(Row r1, Row r2)
    {
        assertEquals(r1.key, r2.key);
        assertEquals(r1.cf, r2.cf);
    }
}
