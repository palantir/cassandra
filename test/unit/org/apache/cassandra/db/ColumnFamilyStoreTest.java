/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.junit.Before;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.cassandra.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.ClearableHistogram;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;
import static junit.framework.Assert.assertNotNull;

@RunWith(OrderedJUnit4ClassRunner.class)
public class ColumnFamilyStoreTest
{
    public static final String KEYSPACE1 = "ColumnFamilyStoreTest1";
    public static final String KEYSPACE2 = "ColumnFamilyStoreTest2";
    public static final String CF_STANDARD1 = "Standard1";
    public static final String CF_STANDARD2 = "Standard2";
    public static final String CF_SUPER1 = "Super1";
    public static final String CF_SUPER6 = "Super6";
    public static final String CF_INDEX1 = "Indexed1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2),
                                    SchemaLoader.keysIndexCFMD(KEYSPACE1, CF_INDEX1, true));
                                    // TODO: Fix superCFMD failing on legacy table creation. Seems to be applying composite comparator to partition key
                                    // SchemaLoader.superCFMD(KEYSPACE1, CF_SUPER1, LongType.instance));
                                    // SchemaLoader.superCFMD(KEYSPACE1, CF_SUPER6, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", LexicalUUIDType.instance, UTF8Type.instance),
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD1));
    }

    @Before
    public void truncateCFS()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2).truncateBlocking();
        // Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_SUPER1).truncateBlocking();

        Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_STANDARD1).truncateBlocking();
    }

    @Test
    public void testMemtableTimestamp() throws Throwable
    {
        assertEquals(Memtable.NO_MIN_TIMESTAMP,
                     (new Memtable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).metadata,
                                   EncodingStats.NO_STATS.minTimestamp))
                     .getMinTimestamp());
    }

    @Test
    // create two sstables, and verify that we only deserialize data from the most recent one
    public void testTimeSortedQuery()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);

        new RowUpdateBuilder(cfs.metadata, 0, "key1")
                .clustering("Column1")
                .add("val", "asdf")
                .build()
                .applyUnsafe();
        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata, 1, "key1")
                .clustering("Column1")
                .add("val", "asdf")
                .build()
                .applyUnsafe();
        cfs.forceBlockingFlush();

        ((ClearableHistogram)cfs.metric.sstablesPerReadHistogram.cf).clear(); // resets counts
        Util.getAll(Util.cmd(cfs, "key1").includeRow("c1").build());
        assertEquals(1, cfs.metric.sstablesPerReadHistogram.cf.getCount());
    }

    @Test
    public void testGetColumnWithWrongBF()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);

        List<Mutation> rms = new LinkedList<>();
        rms.add(new RowUpdateBuilder(cfs.metadata, 0, "key1")
                .clustering("Column1")
                .add("val", "asdf")
                .build());

        Util.writeColumnFamily(rms);

        List<SSTableReader> ssTables = keyspace.getAllSSTables(SSTableSet.LIVE);
        assertEquals(1, ssTables.size());
        ssTables.get(0).forceFilterFailures();
        Util.assertEmpty(Util.cmd(cfs, "key2").build());
    }

    @Test
    public void testEmptyRow() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD2);

        RowUpdateBuilder.deleteRow(cfs.metadata, FBUtilities.timestampMicros(), "key1", "Column1").applyUnsafe();

        Runnable r = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                Row toCheck = Util.getOnlyRowUnfiltered(Util.cmd(cfs, "key1").build());
                Iterator<Cell> iter = toCheck.cells().iterator();
                assert(Iterators.size(iter) == 0);
            }
        };

        reTest(cfs, r);
    }

    // TODO: Implement this once we have hooks to super columns available in CQL context
//    @Test
//    public void testDeleteSuperRowSticksAfterFlush() throws Throwable
//    {
//        String keyspaceName = KEYSPACE1;
//        String cfName= CF_SUPER1;
//
//        Keyspace keyspace = Keyspace.open(keyspaceName);
//        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
//
//        ByteBuffer scfName = ByteBufferUtil.bytes("SuperDuper");
//        DecoratedKey key = Util.dk("flush-resurrection");
//
//        // create an isolated sstable.
//        putColSuper(cfs, key, 0, ByteBufferUtil.bytes("val"), ByteBufferUtil.bytes(1L), ByteBufferUtil.bytes(1L), ByteBufferUtil.bytes("val1"));

//        putColsSuper(cfs, key, scfName,
//                new BufferCell(cellname(1L), ByteBufferUtil.bytes("val1"), 1),
//                new BufferCell(cellname(2L), ByteBufferUtil.bytes("val2"), 1),
//                new BufferCell(cellname(3L), ByteBufferUtil.bytes("val3"), 1));
//        cfs.forceBlockingFlush();
//
//        // insert, don't flush.
//        putColsSuper(cfs, key, scfName,
//                new BufferCell(cellname(4L), ByteBufferUtil.bytes("val4"), 1),
//                new BufferCell(cellname(5L), ByteBufferUtil.bytes("val5"), 1),
//                new BufferCell(cellname(6L), ByteBufferUtil.bytes("val6"), 1));
//
//        // verify insert.
//        final SlicePredicate sp = new SlicePredicate();
//        sp.setSlice_range(new SliceRange());
//        sp.getSlice_range().setCount(100);
//        sp.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
//        sp.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);
//
//        assertRowAndColCount(1, 6, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));
//
//        // delete
//        Mutation rm = new Mutation(keyspace.getName(), key.getKey());
//        rm.deleteRange(cfName, SuperColumns.startOf(scfName), SuperColumns.endOf(scfName), 2);
//        rm.applyUnsafe();
//
//        // verify delete.
//        assertRowAndColCount(1, 0, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));
//
//        // flush
//        cfs.forceBlockingFlush();
//
//        // re-verify delete.
//        assertRowAndColCount(1, 0, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));
//
//        // late insert.
//        putColsSuper(cfs, key, scfName,
//                new BufferCell(cellname(4L), ByteBufferUtil.bytes("val4"), 1L),
//                new BufferCell(cellname(7L), ByteBufferUtil.bytes("val7"), 1L));
//
//        // re-verify delete.
//        assertRowAndColCount(1, 0, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));
//
//        // make sure new writes are recognized.
//        putColsSuper(cfs, key, scfName,
//                new BufferCell(cellname(3L), ByteBufferUtil.bytes("val3"), 3),
//                new BufferCell(cellname(8L), ByteBufferUtil.bytes("val8"), 3),
//                new BufferCell(cellname(9L), ByteBufferUtil.bytes("val9"), 3));
//        assertRowAndColCount(1, 3, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));
//    }

//    private static void assertRowAndColCount(int rowCount, int colCount, boolean isDeleted, Collection<Row> rows) throws CharacterCodingException
//    {
//        assert rows.size() == rowCount : "rowcount " + rows.size();
//        for (Row row : rows)
//        {
//            assert row.cf != null : "cf was null";
//            assert row.cf.getColumnCount() == colCount : "colcount " + row.cf.getColumnCount() + "|" + str(row.cf);
//            if (isDeleted)
//                assert row.cf.isMarkedForDelete() : "cf not marked for delete";
//        }
//    }
//
//    private static String str(ColumnFamily cf) throws CharacterCodingException
//    {
//        StringBuilder sb = new StringBuilder();
//        for (Cell col : cf.getSortedColumns())
//            sb.append(String.format("(%s,%s,%d),", ByteBufferUtil.string(col.name().toByteBuffer()), ByteBufferUtil.string(col.value()), col.timestamp()));
//        return sb.toString();
//    }

    @Test
    public void testDeleteStandardRowSticksAfterFlush() throws Throwable
    {
        // test to make sure flushing after a delete doesn't resurrect delted cols.
        String keyspaceName = KEYSPACE1;
        String cfName = CF_STANDARD1;
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        ByteBuffer col = ByteBufferUtil.bytes("val");
        ByteBuffer val = ByteBufferUtil.bytes("val1");

        // insert
        ColumnDefinition newCol = ColumnDefinition.regularDef(cfs.metadata, ByteBufferUtil.bytes("val2"), AsciiType.instance);
        new RowUpdateBuilder(cfs.metadata, 0, "key1").clustering("Column1").add("val", "val1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "key2").clustering("Column1").add("val", "val1").build().applyUnsafe();
        assertRangeCount(cfs, col, val, 2);

        // flush.
        cfs.forceBlockingFlush();

        // insert, don't flush
        new RowUpdateBuilder(cfs.metadata, 1, "key3").clustering("Column1").add("val", "val1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 1, "key4").clustering("Column1").add("val", "val1").build().applyUnsafe();
        assertRangeCount(cfs, col, val, 4);

        // delete (from sstable and memtable)
        RowUpdateBuilder.deleteRow(cfs.metadata, 5, "key1", "Column1").applyUnsafe();
        RowUpdateBuilder.deleteRow(cfs.metadata, 5, "key3", "Column1").applyUnsafe();

        // verify delete
        assertRangeCount(cfs, col, val, 2);

        // flush
        cfs.forceBlockingFlush();

        // re-verify delete. // first breakage is right here because of CASSANDRA-1837.
        assertRangeCount(cfs, col, val, 2);

        // simulate a 'late' insertion that gets put in after the deletion. should get inserted, but fail on read.
        new RowUpdateBuilder(cfs.metadata, 2, "key1").clustering("Column1").add("val", "val1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 2, "key3").clustering("Column1").add("val", "val1").build().applyUnsafe();

        // should still be nothing there because we deleted this row. 2nd breakage, but was undetected because of 1837.
        assertRangeCount(cfs, col, val, 2);

        // make sure that new writes are recognized.
        new RowUpdateBuilder(cfs.metadata, 10, "key5").clustering("Column1").add("val", "val1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 10, "key6").clustering("Column1").add("val", "val1").build().applyUnsafe();
        assertRangeCount(cfs, col, val, 4);

        // and it remains so after flush. (this wasn't failing before, but it's good to check.)
        cfs.forceBlockingFlush();
        assertRangeCount(cfs, col, val, 4);
    }

    @Test
    public void testClearEphemeralSnapshots() throws Throwable
    {
        // We don't do snapshot-based repair on Windows so we don't have ephemeral snapshots from repair that need clearing.
        // This test will fail as we'll revert to the WindowsFailedSnapshotTracker and counts will be off, but since we
        // don't do snapshot-based repair on Windows, we just skip this test.
        Assume.assumeTrue(!FBUtilities.isWindows);

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_INDEX1);

        //cleanup any previous test gargbage
        cfs.clearSnapshot("");

        int numRows = 1000;
        long[] colValues = new long [numRows * 2]; // each row has two columns
        for (int i = 0; i < colValues.length; i+=2)
        {
            colValues[i] = (i % 4 == 0 ? 1L : 2L); // index column
            colValues[i+1] = 3L; //other column
        }
        ScrubTest.fillIndexCF(cfs, false, colValues);

        cfs.snapshot("nonEphemeralSnapshot", null, false, false);
        cfs.snapshot("ephemeralSnapshot", null, true, false);

        Map<String, Pair<Long, Long>> snapshotDetails = cfs.getSnapshotDetails();
        assertEquals(2, snapshotDetails.size());
        assertTrue(snapshotDetails.containsKey("ephemeralSnapshot"));
        assertTrue(snapshotDetails.containsKey("nonEphemeralSnapshot"));

        ColumnFamilyStore.clearEphemeralSnapshots(cfs.getDirectories());

        snapshotDetails = cfs.getSnapshotDetails();
        assertEquals(1, snapshotDetails.size());
        assertTrue(snapshotDetails.containsKey("nonEphemeralSnapshot"));

        //test cleanup
        cfs.clearSnapshot("");
    }

    @Test
    public void testSnapshotSize()
    {
        // cleanup any previous test gargbage
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        cfs.clearSnapshot("");

        // Add row
        new RowUpdateBuilder(cfs.metadata, 0, "key1")
        .clustering("Column1")
        .add("val", "asdf")
        .build()
        .applyUnsafe();
        cfs.forceBlockingFlush();

        // snapshot
        cfs.snapshot("basic", null, false, false);

        // check snapshot was created
        Map<String, Pair<Long, Long>> snapshotDetails = cfs.getSnapshotDetails();
        assertThat(snapshotDetails).hasSize(1);
        assertThat(snapshotDetails).containsKey("basic");

        // check that sizeOnDisk > trueSize = 0
        Pair<Long, Long> details = snapshotDetails.get("basic");
        long sizeOnDisk = details.left;
        long trueSize = details.right;
        assertThat(sizeOnDisk).isGreaterThan(trueSize);
        assertThat(trueSize).isZero();

        // compact base table to make trueSize > 0
        cfs.forceMajorCompaction();
        LifecycleTransaction.waitForDeletions();

        // sizeOnDisk > trueSize because trueSize does not include manifest.json
        // Check that truesize now is > 0
        snapshotDetails = cfs.getSnapshotDetails();
        details = snapshotDetails.get("basic");
        sizeOnDisk = details.left;
        trueSize = details.right;
        assertThat(sizeOnDisk).isGreaterThan(trueSize);
        assertThat(trueSize).isPositive();
    }

    @Test
    public void testBackupAfterFlush() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_STANDARD1);
        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key1")).clustering("Column1").add("val", "asdf").build().applyUnsafe();
        cfs.forceBlockingFlush();
        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key2")).clustering("Column1").add("val", "asdf").build().applyUnsafe();
        cfs.forceBlockingFlush();

        for (int version = 1; version <= 2; ++version)
        {
            Descriptor existing = new Descriptor(cfs.getDirectories().getDirectoryForNewSSTables(), KEYSPACE2, CF_STANDARD1, version,
                                                 SSTableFormat.Type.BIG);
            Descriptor desc = new Descriptor(Directories.getBackupsDirectory(existing), KEYSPACE2, CF_STANDARD1, version, SSTableFormat.Type.BIG);
            for (Component c : new Component[]{ Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.STATS })
                assertTrue("Cannot find backed-up file:" + desc.filenameFor(c), new File(desc.filenameFor(c)).exists());
        }
    }

    @Test
    public void testGetRepairedAtPerSstable() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        new RowUpdateBuilder(cfs.metadata, 1, "key1").clustering("name").add("val", "1").build().applyUnsafe();
        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata, 2, "key2").clustering("name").add("val", "2").build().applyUnsafe();
        cfs.forceBlockingFlush();

        assertEquals(getUnrepairedSSTables(cfs).size(), 2);

        Iterator<SSTableReader> sstables = getUnrepairedSSTables(cfs).iterator();
        SSTableReader sstableToRepair = sstables.next();
        sstableToRepair.descriptor.getMetadataSerializer().mutateRepairedAt(sstableToRepair.descriptor, 1234567L);
        sstableToRepair.reloadSSTableMetadata();
        String repairedSstable = sstableToRepair.descriptor.relativeFilenameFor(Component.DATA);

        Map<String, Long> repairedAtPerSstable = cfs.getRepairedAtPerSstable();
        assertTrue(repairedAtPerSstable.containsKey(repairedSstable));
        assertEquals(1234567L, repairedAtPerSstable.get(repairedSstable).longValue());

        String unrepairedSstable = sstables.next().descriptor.relativeFilenameFor(Component.DATA);
        assertTrue(repairedAtPerSstable.containsKey(unrepairedSstable));
        assertEquals(0L, repairedAtPerSstable.get(unrepairedSstable).longValue());
    }

    // TODO: Fix once we have working supercolumns in 8099
//    // CASSANDRA-3467.  the key here is that supercolumn and subcolumn comparators are different
//    @Test
//    public void testSliceByNamesCommandOnUUIDTypeSCF() throws Throwable
//    {
//        String keyspaceName = KEYSPACE1;
//        String cfName = CF_SUPER6;
//        ByteBuffer superColName = LexicalUUIDType.instance.fromString("a4ed3562-0e8e-4b41-bdfd-c45a2774682d");
//        Keyspace keyspace = Keyspace.open(keyspaceName);
//        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
//        DecoratedKey key = Util.dk("slice-get-uuid-type");
//
//        // Insert a row with one supercolumn and multiple subcolumns
//        putColsSuper(cfs, key, superColName, new BufferCell(cellname("a"), ByteBufferUtil.bytes("A"), 1),
//                                             new BufferCell(cellname("b"), ByteBufferUtil.bytes("B"), 1));
//
//        // Get the entire supercolumn like normal
//        ColumnFamily cfGet = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, cfName, System.currentTimeMillis()));
//        assertEquals(ByteBufferUtil.bytes("A"), cfGet.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("a"))).value());
//        assertEquals(ByteBufferUtil.bytes("B"), cfGet.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("b"))).value());
//
//        // Now do the SliceByNamesCommand on the supercolumn, passing both subcolumns in as columns to get
//        SortedSet<CellName> sliceColNames = new TreeSet<CellName>(cfs.metadata.comparator);
//        sliceColNames.add(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("a")));
//        sliceColNames.add(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("b")));
//        SliceByNamesReadCommand cmd = new SliceByNamesReadCommand(keyspaceName, key.getKey(), cfName, System.currentTimeMillis(), new NamesQueryFilter(sliceColNames));
//        ColumnFamily cfSliced = cmd.getRow(keyspace).cf;
//
//        // Make sure the slice returns the same as the straight get
//        assertEquals(ByteBufferUtil.bytes("A"), cfSliced.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("a"))).value());
//        assertEquals(ByteBufferUtil.bytes("B"), cfSliced.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("b"))).value());
//    }


    // TODO: Fix once SSTableSimpleWriter's back in
    // @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-6086">CASSANDRA-6086</a>


    // TODO: Fix once SSTableSimpleWriter's back in
//    @Test
//    public void testLoadNewSSTablesAvoidsOverwrites() throws Throwable
//    {
//        String ks = KEYSPACE1;
//        String cf = CF_STANDARD1;
//        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(cf);
//        SSTableDeletingTask.waitForDeletions();
//
//        final CFMetaData cfmeta = Schema.instance.getCFMetaData(ks, cf);
//        Directories dir = new Directories(cfs.metadata);
//
//        // clear old SSTables (probably left by CFS.clearUnsafe() calls in other tests)
//        for (Map.Entry<Descriptor, Set<Component>> entry : dir.sstableLister().list().entrySet())
//        {
//            for (Component component : entry.getValue())
//            {
//                FileUtils.delete(entry.getKey().filenameFor(component));
//            }
//        }
//
//        // sanity check
//        int existingSSTables = dir.sstableLister().list().keySet().size();
//        assert existingSSTables == 0 : String.format("%d SSTables unexpectedly exist", existingSSTables);
//
//        ByteBuffer key = bytes("key");
//
//        SSTableSimpleWriter writer = new SSTableSimpleWriter(dir.getDirectoryForNewSSTables(),
//                                                             cfmeta, StorageService.getPartitioner())
//        {
//            @Override
//            protected SSTableWriter getWriter()
//            {
//                // hack for reset generation
//                generation.set(0);
//                return super.getWriter();
//            }
//        };
//        writer.newRow(key);
//        writer.addColumn(bytes("col"), bytes("val"), 1);
//        writer.close();
//
//        writer = new SSTableSimpleWriter(dir.getDirectoryForNewSSTables(),
//                                         cfmeta, StorageService.getPartitioner());
//        writer.newRow(key);
//        writer.addColumn(bytes("col"), bytes("val"), 1);
//        writer.close();
//
//        Set<Integer> generations = new HashSet<>();
//        for (Descriptor descriptor : dir.sstableLister().list().keySet())
//            generations.add(descriptor.generation);
//
//        // we should have two generations: [1, 2]
//        assertEquals(2, generations.size());
//        assertTrue(generations.contains(1));
//        assertTrue(generations.contains(2));
//
//        assertEquals(0, cfs.getLiveSSTables().size());
//
//        // start the generation counter at 1 again (other tests have incremented it already)
//        cfs.resetFileIndexGenerator();
//
//        boolean incrementalBackupsEnabled = DatabaseDescriptor.isIncrementalBackupsEnabled();
//        try
//        {
//            // avoid duplicate hardlinks to incremental backups
//            DatabaseDescriptor.setIncrementalBackupsEnabled(false);
//            cfs.loadNewSSTables();
//        }
//        finally
//        {
//            DatabaseDescriptor.setIncrementalBackupsEnabled(incrementalBackupsEnabled);
//        }
//
//        assertEquals(2, cfs.getLiveSSTables().size());
//        generations = new HashSet<>();
//        for (Descriptor descriptor : dir.sstableLister().list().keySet())
//            generations.add(descriptor.generation);
//
//        // normally they would get renamed to generations 1 and 2, but since those filenames already exist,
//        // they get skipped and we end up with generations 3 and 4
//        assertEquals(2, generations.size());
//        assertTrue(generations.contains(3));
//        assertTrue(generations.contains(4));
//    }

    public void reTest(ColumnFamilyStore cfs, Runnable verify) throws Exception
    {
        verify.run();
        cfs.forceBlockingFlush();
        verify.run();
    }

    private void assertRangeCount(ColumnFamilyStore cfs, ByteBuffer col, ByteBuffer val, int count)
    {
        assertRangeCount(cfs, cfs.metadata.getColumnDefinition(col), val, count);
    }

    private void assertRangeCount(ColumnFamilyStore cfs, ColumnDefinition col, ByteBuffer val, int count)
    {

        int found = 0;
        if (count != 0)
        {
            for (FilteredPartition partition : Util.getAll(Util.cmd(cfs).filterOn(col.name.toString(), Operator.EQ, val).build()))
            {
                for (Row r : partition)
                {
                    if (r.getCell(col).value().equals(val))
                        ++found;
                }
            }
        }
        assertEquals(count, found);
    }

    @Test
    public void testSnapshotWithoutFlushWithSecondaryIndexes() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_INDEX1);
        cfs.truncateBlocking();

        UpdateBuilder builder = UpdateBuilder.create(cfs.metadata, "key")
                                             .newRow()
                                             .add("birthdate", 1L)
                                             .add("notbirthdate", 2L);
        new Mutation(builder.build()).applyUnsafe();
        cfs.forceBlockingFlush();

        String snapshotName = "newSnapshot";
        cfs.snapshotWithoutFlush(snapshotName);

        File snapshotManifestFile = cfs.getDirectories().getSnapshotManifestFile(snapshotName);
        JSONParser parser = new JSONParser();
        JSONObject manifest = (JSONObject) parser.parse(new FileReader(snapshotManifestFile));
        JSONArray files = (JSONArray) manifest.get("files");

        // Keyspace1-Indexed1 and the corresponding index
        assert files.size() == 2;

        // Snapshot of the secondary index is stored in the subfolder with the same file name
        String baseTableFile = (String) files.get(0);
        String indexTableFile = (String) files.get(1);
        assert !baseTableFile.equals(indexTableFile);
        assert Directories.isSecondaryIndexFolder(new File(indexTableFile).getParentFile());
        assert indexTableFile.endsWith(baseTableFile);
    }

    @Test
    public void testScrubDataDirectories() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);

        ColumnFamilyStore.scrubDataDirectories(cfs.metadata);

        new RowUpdateBuilder(cfs.metadata, 2, "key").clustering("name").add("val", "2").build().applyUnsafe();
        cfs.forceBlockingFlush();

        // Nuke the metadata and reload that sstable
        Collection<SSTableReader> ssTables = cfs.getLiveSSTables();
        assertEquals(1, ssTables.size());
        SSTableReader ssTable = ssTables.iterator().next();

        String dataFileName = ssTable.descriptor.filenameFor(Component.DATA);
        String tmpDataFileName = ssTable.descriptor.tmpFilenameFor(Component.DATA);
        new File(dataFileName).renameTo(new File(tmpDataFileName));

        ssTable.selfRef().release();

        ColumnFamilyStore.scrubDataDirectories(cfs.metadata);

        List<File> ssTableFiles = new Directories(cfs.metadata).sstableLister(Directories.OnTxnErr.THROW).listFiles();
        assertNotNull(ssTableFiles);
        assertEquals(0, ssTableFiles.size());
    }

    private static Set<SSTableReader> getUnrepairedSSTables(ColumnFamilyStore cfs)
    {
        return ImmutableSet.copyOf(cfs.getTracker().getView().sstables(SSTableSet.LIVE, (s) -> !s.isRepaired()));
    }
}
