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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;

import com.google.common.base.Supplier;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;
import static org.junit.Assert.assertEquals;

public class ColumnFamilyMetricTest
{

    public static final byte[] EMPTY_BYTES = new byte[0];

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("Keyspace1",
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD("Keyspace1", "Standard2"));
    }

    @Test
    public void testSizeMetric()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        final ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard2");
        store.disableAutoCompaction();

        store.truncateBlocking();

        assertEquals(0, store.metric.liveDiskSpaceUsed.getCount());
        assertEquals(0, store.metric.totalDiskSpaceUsed.getCount());

        for (int j = 0; j < 10; j++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(String.valueOf(j));
            Mutation rm = new Mutation("Keyspace1", key);
            rm.add("Standard2", cellname("0"), ByteBufferUtil.EMPTY_BYTE_BUFFER, j);
            rm.apply();
        }
        store.forceBlockingFlush();
        Collection<SSTableReader> sstables = store.getSSTables();
        long size = 0;
        for (SSTableReader reader : sstables)
        {
            size += reader.bytesOnDisk();
        }

        // size metrics should show the sum of all SSTable sizes
        assertEquals(size, store.metric.liveDiskSpaceUsed.getCount());
        assertEquals(size, store.metric.totalDiskSpaceUsed.getCount());

        store.truncateBlocking();

        // after truncate, size metrics should be down to 0
        Util.spinAssertEquals(
                0L,
                new Supplier<Object>()
                {
                    public Long get()
                    {
                        return store.metric.liveDiskSpaceUsed.getCount();
                    }
                },
                30);
        Util.spinAssertEquals(
                0L,
                new Supplier<Object>()
                {
                    public Long get()
                    {
                        return store.metric.totalDiskSpaceUsed.getCount();
                    }
                },
                30);

        store.enableAutoCompaction();
    }

    @Test
    public void testColUpdateTimeDeltaFiltering()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard2");

        // This confirms another test/set up did not overflow the histogram
        store.metric.colUpdateTimeDeltaHistogram.cf.getSnapshot().get999thPercentile();

        ByteBuffer key = ByteBufferUtil.bytes(4242);
        Mutation m = new Mutation("Keyspace1", key);
        m.add("Standard2", cellname("0"), ByteBufferUtil.bytes("0"), 0);
        m.apply();

        // The histogram should not have overflowed on the first write
        store.metric.colUpdateTimeDeltaHistogram.cf.getSnapshot().get999thPercentile();

        m = new Mutation("Keyspace1", key);
        // smallest time delta that would overflow the histogram if unfiltered
        m.add("Standard2", cellname("0"), ByteBufferUtil.bytes("1"), 18165375903307L);
        m.apply();

        // CASSANDRA-11117 - update with large timestamp delta should not overflow the histogram
        store.metric.colUpdateTimeDeltaHistogram.cf.getSnapshot().get999thPercentile();
    }

    @Test
    public void testTombstoneWarnings() throws IOException
    {
        DatabaseDescriptor.setTombstoneWarnThreshold(10);
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard2");

        store.disableAutoCompaction();

        store.truncateBlocking();

        assertEquals(0, store.metric.tombstoneWarnings.getCount());

        ByteBuffer key = ByteBufferUtil.bytes("key");
        for (int j = 0; j < 11; j++)
        {
            Mutation rm = new Mutation("Keyspace1", key);
            rm.add("Standard2", cellname(j), ByteBufferUtil.EMPTY_BYTE_BUFFER, j);
            rm.apply();
        }

        Mutation write = new Mutation("Keyspace1", key);
        write.add("Standard2", cellname(11), ByteBufferUtil.EMPTY_BYTE_BUFFER, 11);
        write.apply();

        for (int j = 0; j < 11; j++)
        {
            Mutation rm = new Mutation("Keyspace1", key);
            rm.delete("Standard2", cellname(j), j);
            rm.apply();
        }

        store.forceBlockingFlush();

        assertEquals(0, store.metric.tombstoneWarnings.getCount());

        Composite unbounded = validateColumnFamily("Keyspace1", "Standard2").comparator.fromByteBuffer(ByteBuffer.wrap(EMPTY_BYTES));
        DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
        SliceQueryFilter filter = new SliceQueryFilter(unbounded, unbounded, false, Integer.MAX_VALUE);
        keyspace.getRow(new QueryFilter(dk, "Standard2", filter, Long.MAX_VALUE));
        keyspace.getRow(new QueryFilter(dk, "Standard2", filter, Long.MAX_VALUE));
        keyspace.getRow(new QueryFilter(dk, "Standard2", filter, Long.MAX_VALUE));

        assertEquals(3, store.metric.tombstoneWarnings.getCount());
    }

    @Test
    public void testRepairedAtSSTableCount() throws IOException
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard2");

        store.disableAutoCompaction();

        store.truncateBlocking();

        Mutation rm;
        for (int i = 0; i < 5; i++)
        {
            rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("key1"));
            rm.add("Standard2", cellname("Column" + i), ByteBufferUtil.bytes("value"), i);
            rm.applyUnsafe();
            store.forceBlockingFlush();
        }

        assertEquals(store.getUnrepairedSSTables().size(), 5);
        assertEquals(0, store.metric.repairedAtSSTableCount.getValue().longValue());

        Iterator<SSTableReader> sstables = store.getUnrepairedSSTables().iterator();
        SSTableReader sstableToRepair = sstables.next();
        sstableToRepair.descriptor.getMetadataSerializer().mutateRepairedAt(sstableToRepair.descriptor, 1234567L);
        sstableToRepair.reloadSSTableMetadata();

        assertEquals(1, store.metric.repairedAtSSTableCount.getValue().longValue());

        while (sstables.hasNext())
        {
            sstableToRepair = sstables.next();
            sstableToRepair.descriptor.getMetadataSerializer().mutateRepairedAt(sstableToRepair.descriptor, 1234567L);
            sstableToRepair.reloadSSTableMetadata();
        }

        assertEquals(5, store.metric.repairedAtSSTableCount.getValue().longValue());
    }
}
