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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TombstoneCountingIterator implements Iterator<OnDiskAtom>
{
    private final Iterator<? extends  OnDiskAtom> delegate;
    private final int gcBefore;
    private final ColumnFamily returnCF;

    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyStore.class);

    private TombstoneCountingIterator(int gcBefore, ColumnFamily returnCF, Iterator<? extends  OnDiskAtom> delegate) {
        this.delegate = delegate;
        this.gcBefore = gcBefore;
        this.returnCF = returnCF;
    }

    public static TombstoneCountingIterator wrapIterator(int gcBefore, ColumnFamily returnCF, Iterator<? extends OnDiskAtom> delegate) {
        return new TombstoneCountingIterator(gcBefore, returnCF, delegate);
    }

    public boolean hasNext()
    {
        return delegate.hasNext();
    }

    public OnDiskAtom next()
    {
        OnDiskAtom onDiskAtom = delegate.next();

        DeletionInfo deletionInfo = returnCF.deletionInfo();

        logger.trace("Maybe counting cell as range tombstone", onDiskAtom instanceof RangeTombstone,
                     deletionInfo.getRangeTombstoneCounter().getNonDroppableCount(),
                     deletionInfo.getRangeTombstoneCounter().getDroppableCount());

        if (onDiskAtom instanceof RangeTombstone) {

            if (((RangeTombstone)onDiskAtom).data.isGcAble(gcBefore)) {
                deletionInfo.getRangeTombstoneCounter().incrementDroppable();
            } else {
                deletionInfo.getRangeTombstoneCounter().incrementNonDroppable();
            }
        }

        if (delegate instanceof SSTableAwareOnDiskAtomIterator) {

            SSTableReader ssTableReader = ((SSTableAwareOnDiskAtomIterator) delegate).getSSTableReader();

            if (onDiskAtom instanceof Cell)  {
                Cell cell = (Cell)onDiskAtom;
                if (!cell.isLive(System.currentTimeMillis())
                        && !ExpiringCell.class.isAssignableFrom(cell.getClass())
                        && cell.getLocalDeletionTime() < gcBefore) {
                    ssTableReader.incrementTombstoneMeter();
                }
            }
        }

        return onDiskAtom;
    }

    public void remove()
    {
        delegate.remove();
    }
}
