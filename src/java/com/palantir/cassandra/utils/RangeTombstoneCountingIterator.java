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

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;

public class RangeTombstoneCountingIterator implements Iterator<OnDiskAtom>
{
    private final Iterator<? extends  OnDiskAtom> delegate;
    private final int gcBefore;
    private final ColumnFamily returnCF;

    private RangeTombstoneCountingIterator(int gcBefore, ColumnFamily returnCF, Iterator<? extends  OnDiskAtom> delegate) {
        this.delegate = delegate;
        this.gcBefore = gcBefore;
        this.returnCF = returnCF;
    }

    public static RangeTombstoneCountingIterator wrapIterator(int gcBefore, ColumnFamily returnCF, Iterator<? extends OnDiskAtom> delegate) {
        return new RangeTombstoneCountingIterator(gcBefore, returnCF, delegate);
    }

    public boolean hasNext()
    {
        return delegate.hasNext();
    }

    public OnDiskAtom next()
    {
        OnDiskAtom onDiskAtom = delegate.next();

        if (onDiskAtom instanceof RangeTombstone) {
            this.returnCF.getRangeTombstoneCounter().increment();

            if (((RangeTombstone)onDiskAtom).data.isGcAble(gcBefore)) {
                this.returnCF.getRangeTombstoneCounter().incrementDroppable();
            }
        }

        return onDiskAtom;
    }

    public void remove()
    {
        delegate.remove();
    }
}
