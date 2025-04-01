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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.FilterExperiment;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.restrictions.MultiColumnRestriction;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ColumnStats;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.cellname;
import static org.junit.Assert.assertEquals;

public class CollationControllerTest
{
    private static final String KEYSPACE1 = "CollationControllerTest";
    private static final String CF = "Standard1";
    private static final String CFGCGRACE = "StandardGCGS0";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CFGCGRACE).gcGraceSeconds(0));
    }

    @Test
    public void expiredRangeTombstone() {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        DecoratedKey key = Util.dk("key1");

        Mutation delete = new Mutation(keyspace.getName(), key.getKey());
        delete.deleteRange(cfs.name, Util.cellname("Column1"), Util.cellname("Column1"), 10);
        delete.apply();

        Mutation mutate = new Mutation(keyspace.getName(), key.getKey());
        mutate.add(cfs.name, Util.cellname("Column1"),  ByteBufferUtil.bytes("asdf"), 0);
        mutate.applyUnsafe();

        QueryFilter filter = new QueryFilter(key, CF, new IdentityQueryFilter(), 864000);
        ColumnFamily cf = cfs.getColumnFamily(filter);
        List<Iterator<? extends OnDiskAtom>> iterators = new ArrayList<>();
        iterators.add(new IdentityQueryFilter().getColumnIterator(key, cf));
        filter.collateOnDiskAtom(cf, iterators, Integer.MIN_VALUE);

        cf.iterator().forEachRemaining(cell -> {
            if (cell != null) {
                System.out.println(cell.getString(cf.getComparator()));
            }
        });
    }

    @Test
    public void expiredRangeTombstoneIsRemoved() {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        DecoratedKey key = Util.dk("key1");

        Mutation mutation2 = new Mutation(keyspace.getName(), key.getKey());
        mutation2.add(cfs.name, Util.cellname("Column1"),  ByteBufferUtil.bytes("asdf"), 0);
        mutation2.applyUnsafe();


        Mutation mutation = new Mutation(keyspace.getName(), key.getKey());
        mutation.deleteRange(cfs.name, Util.cellname("Column1"), Util.cellname("Column1"), 10);
        mutation.apply();

        QueryFilter filter = new QueryFilter(key, CF, new IdentityQueryFilter(), 864000);
        ColumnFamily cf = cfs.getColumnFamily(filter);

        System.out.println("HERE");
        cf.iterator().forEachRemaining(cell -> {
            if (cell != null) {
                System.out.println(cell.getString(cf.getComparator()));
            }
        });
    }

    @Test
    public void getTopLevelColumnsSkipsSSTablesModifiedBeforeRowDelete() 
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        Mutation rm;
        DecoratedKey dk = Util.dk("key1");
        
        // add data
        rm = new Mutation(keyspace.getName(), dk.getKey());
        rm.add(cfs.name, Util.cellname("Column1"), ByteBufferUtil.bytes("asdf"), 0);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();
        
        // remove
        rm = new Mutation(keyspace.getName(), dk.getKey());
        rm.delete(cfs.name, 10);
        rm.applyUnsafe();
        
        // add another mutation because sstable maxtimestamp isn't set
        // correctly during flush if the most recent mutation is a row delete
        rm = new Mutation(keyspace.getName(), Util.dk("key2").getKey());
        rm.add(cfs.name, Util.cellname("Column1"), ByteBufferUtil.bytes("zxcv"), 20);
        rm.applyUnsafe();
        
        cfs.forceBlockingFlush();

        // add yet one more mutation
        rm = new Mutation(keyspace.getName(), dk.getKey());
        rm.add(cfs.name, Util.cellname("Column1"), ByteBufferUtil.bytes("foobar"), 30);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        // A NamesQueryFilter goes down one code path (through collectTimeOrderedData())
        // It should only iterate the last flushed sstable, since it probably contains the most recent value for Column1
        QueryFilter filter = Util.namesQueryFilter(cfs, dk, "Column1");
        CollationController controller = new CollationController(cfs, filter, Integer.MIN_VALUE);
        controller.getTopLevelColumns(true, FilterExperiment.USE_OPTIMIZED);
        assertEquals(1, controller.getSstablesIterated());

        // SliceQueryFilter goes down another path (through collectAllData())
        // We will read "only" the last sstable in that case, but because the 2nd sstable has a tombstone that is more
        // recent than the maxTimestamp of the very first sstable we flushed, we should only read the 2 first sstables.
        filter = QueryFilter.getIdentityFilter(dk, cfs.name, System.currentTimeMillis());
        controller = new CollationController(cfs, filter, Integer.MIN_VALUE);
        ColumnFamily cf = controller.getTopLevelColumns(true, FilterExperiment.USE_OPTIMIZED);
        assertEquals(2, controller.getSstablesIterated());
        cf.iterator().forEachRemaining(cell -> {
            System.out.println("Yo");
            if (cell != null) System.out.println(cell.getString(cf.getComparator()));
        });
    }

    @Test
    public void ensureTombstonesAppliedAfterGCGS()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CFGCGRACE);
        cfs.disableAutoCompaction();

        Mutation rm;
        DecoratedKey dk = Util.dk("key1");
        CellName cellName = Util.cellname("Column1");

        // add data
        rm = new Mutation(keyspace.getName(), dk.getKey());
        rm.add(cfs.name, cellName, ByteBufferUtil.bytes("asdf"), 0);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        // remove
        rm = new Mutation(keyspace.getName(), dk.getKey());
        rm.delete(cfs.name, cellName, 0);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        // use "realistic" query times since we'll compare these numbers to the local deletion time of the tombstone
        QueryFilter filter;
        long queryAt = System.currentTimeMillis() + 1000;
        int gcBefore = cfs.gcBefore(queryAt);

        filter = QueryFilter.getNamesFilter(dk, cfs.name, FBUtilities.singleton(cellName, cfs.getComparator()), queryAt);
        CollationController controller = new CollationController(cfs, filter, gcBefore);
        assert ColumnFamilyStore.removeDeleted(controller.getTopLevelColumns(true, FilterExperiment.USE_OPTIMIZED), gcBefore) == null;

        filter = QueryFilter.getIdentityFilter(dk, cfs.name, queryAt);
        controller = new CollationController(cfs, filter, gcBefore);
        assert ColumnFamilyStore.removeDeleted(controller.getTopLevelColumns(true, FilterExperiment.USE_OPTIMIZED), gcBefore) == null;
    }
}
