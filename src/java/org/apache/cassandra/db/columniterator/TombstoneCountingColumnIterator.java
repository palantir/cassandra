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

package org.apache.cassandra.db.columniterator;

import java.io.IOException;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletedCell;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class TombstoneCountingColumnIterator implements OnDiskAtomIterator
{
    private final SSTableReader reader;
    private final OnDiskAtomIterator delegate;
    private int tombstonesRead = 0;

    public TombstoneCountingColumnIterator(SSTableReader reader, OnDiskAtomIterator delegate)
    {
        this.reader = reader;
        this.delegate = delegate;
    }

    public ColumnFamily getColumnFamily()
    {
        return delegate.getColumnFamily();
    }

    public DecoratedKey getKey()
    {
        return delegate.getKey();
    }

    public void close() throws IOException
    {
        reader.incrementTombstonesRead(tombstonesRead);
        delegate.close();
    }

    public boolean hasNext()
    {
        return delegate.hasNext();
    }

    public OnDiskAtom next()
    {
        OnDiskAtom atom = delegate.next();
        if (isTombstone(atom)) {
            ++tombstonesRead;
        }
        return atom;
    }

    private static boolean isTombstone(OnDiskAtom atom) {
        return atom instanceof RangeTombstone | atom instanceof DeletedCell;
    }
}
