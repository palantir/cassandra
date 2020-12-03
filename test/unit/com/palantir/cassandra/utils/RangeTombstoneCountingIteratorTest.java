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

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.composites.Composite;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RangeTombstoneCountingIteratorTest
{

    private static final int GC_GRACE = 2000;

    private ColumnFamily columnFamily;
    RangeTombstoneCounter counter;

    @Before
    public void setup() {
        columnFamily = mock(ColumnFamily.class);
        counter = new RangeTombstoneCounter();
        when(columnFamily.getRangeTombstoneCounter()).thenReturn(counter);
    }

    @Test
    public void delegatesAllMethodCalls()
    {
        Iterator<? extends OnDiskAtom> delegate = mock(Iterator.class);
        RangeTombstoneCountingIterator iterator =
            RangeTombstoneCountingIterator.wrapIterator(0, columnFamily, delegate);
        iterator.hasNext();
        Mockito.verify(delegate).hasNext();
        iterator.next();
        Mockito.verify(delegate).next();
        iterator.remove();
        Mockito.verify(delegate).remove();
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

        RangeTombstoneCountingIterator iterator = RangeTombstoneCountingIterator.wrapIterator(GC_GRACE,
                                                                                              columnFamily,
                                                                                              delegate);
        while(iterator.hasNext()) {
            iterator.next();
        }
        assertThat(counter.getCount()).isEqualTo(3);
        assertThat(counter.getDroppableCount()).isEqualTo(1);
    }
}
