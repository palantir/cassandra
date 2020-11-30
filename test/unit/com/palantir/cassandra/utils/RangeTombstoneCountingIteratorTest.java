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
import org.mockito.Mockito;

public class RangeTombstoneCountingIteratorTest
{

    @Test
    public void delegatesAllMethodCalls() {
        Iterator<? extends OnDiskAtom> delegate = Mockito.mock(Iterator.class);
        Mockito.when(delegate.next()).thenReturn(Mockito.mock(Cell.class));
        RangeTombstoneCountingIterator iterator = RangeTombstoneCountingIterator.wrapIterator(0,
                                                                                              Mockito.mock(ColumnFamily.class),
                                                                                              delegate);
        iterator.hasNext();
        Mockito.verify(delegate.hasNext());
        iterator.next();
        Mockito.verify(delegate.next());
        iterator.remove();
        Mockito.verify(delegate).remove();
    }
}
