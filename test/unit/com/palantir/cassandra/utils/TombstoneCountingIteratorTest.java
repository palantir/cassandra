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

package com.palantir.cassandra.utils;

import java.util.Iterator;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.SSTableAwareOnDiskAtomIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.SSTableNamesIterator;
import org.apache.cassandra.io.sstable.format.big.SSTableSliceIterator;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.composites.Composite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TombstoneCountingIteratorTest
{

    private static final int GC_GRACE = 2000;
    private static final int TTL = 5000;

    private ColumnFamily columnFamily;
    private DeletionInfo deletionInfo;
    RangeTombstoneCounter counter;

    @Before
    public void setup() {
        columnFamily = mock(ColumnFamily.class);
        deletionInfo = mock(DeletionInfo.class);

        counter = new RangeTombstoneCounter();
        when(columnFamily.deletionInfo()).thenReturn(deletionInfo);
        when(deletionInfo.getRangeTombstoneCounter()).thenReturn(counter);
    }

    @Test
    public void delegatesAllMethodCalls()
    {
        Iterator<? extends OnDiskAtom> delegate = mock(Iterator.class);
        TombstoneCountingIterator iterator =
            TombstoneCountingIterator.wrapIterator(0, columnFamily, delegate);
        iterator.hasNext();
        verify(delegate).hasNext();
        iterator.next();
        verify(delegate).next();
        iterator.remove();
        verify(delegate).remove();
    }

    @Test
    public void countsRangeTombstones()
    {
        Iterator<? extends OnDiskAtom> delegate = mock(Iterator.class);
        long timestamp = System.currentTimeMillis();
        RangeTombstone droppableTombstone =
            new RangeTombstone(mock(Composite.class), mock(Composite.class), timestamp - GC_GRACE, 0);
        RangeTombstone tombstone = new RangeTombstone(mock(Composite.class), mock(Composite.class), timestamp, GC_GRACE);
        when(delegate.next()).thenReturn(tombstone, droppableTombstone, tombstone);
        when(delegate.hasNext()).thenReturn(true, true, true, false);

        TombstoneCountingIterator iterator = TombstoneCountingIterator.wrapIterator(GC_GRACE,
                                                                                              columnFamily,
                                                                                              delegate);
        while(iterator.hasNext()) {
            iterator.next();
        }
        assertThat(counter.getNonDroppableCount()).isEqualTo(2);
        assertThat(counter.getDroppableCount()).isEqualTo(1);
    }

    @Test
    public void incrementsTombstoneMeterForSSTableNamesIterator()
    {

        Cell tombstone = mock(Cell.class);
        when(tombstone.isLive(anyLong())).thenReturn(false);
        when(tombstone.getLocalDeletionTime()).thenReturn(GC_GRACE - 100);

        Cell nonTombstone = mock(Cell.class);
        when(nonTombstone.isLive(anyLong())).thenReturn(true);

        SSTableAwareOnDiskAtomIterator delegate = mock(SSTableAwareOnDiskAtomIterator.class);
        SSTableReader reader = mock(SSTableReader.class);
        when(delegate.getSSTableReader()).thenReturn(reader);

        when(delegate.hasNext()).thenReturn(true, true, true, false);
        when(delegate.next()).thenReturn(tombstone, tombstone, nonTombstone);
        TombstoneCountingIterator iterator = TombstoneCountingIterator.wrapIterator(GC_GRACE,
                columnFamily,
                delegate);

        while(iterator.hasNext()) {
            iterator.next();
        }
        verify(reader, times(2)).incrementTombstoneMeter();
    }
}
