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

import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RangeTombstone;

public class RangeTombstoneCounter
{
    private final int droppableRangeTombstones;
    private final int rangeTombstones;

    public static final RangeTombstoneCounter EMPTY = new RangeTombstoneCounter(0, 0);

    public RangeTombstoneCounter(int droppableRangeTombstones, int rangeTombstones) {
        this.droppableRangeTombstones = droppableRangeTombstones;
        this.rangeTombstones = rangeTombstones;
    }

    public static RangeTombstoneCounter create(int gcBefore, DeletionInfo deletionInfo) {
        Iterator<RangeTombstone> rangeTombstoneIterator = deletionInfo.rangeIterator();
        int droppable = 0;
        int count = 0;
        while (rangeTombstoneIterator.hasNext()) {
            count++;
            RangeTombstone rangeTombstone = rangeTombstoneIterator.next();

            if (rangeTombstone.data.isGcAble(gcBefore)) {
                droppable++;
            }
        }
        return new RangeTombstoneCounter(droppable, count - droppable);
    }

    public int getDroppableRangeTombstones()
    {
        return droppableRangeTombstones;
    }

    public int getRangeTombstones()
    {
        return rangeTombstones;
    }
}
