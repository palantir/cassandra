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

package com.palantir.cassandra.db.compaction;

import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.io.sstable.Descriptor;


public interface IColumnFamilyStoreWriteAheadLogger
{
    /**
     * Mark {@code descriptors} as a set of SSTables in {@code cfMetaData} needing to be atomically deleted.
     *
     * This method is assumed to be thread-safe.
     * Implementors are resposible for synchronization, and treat {@code cfMetaData} as the smallest unit on which to
     * synchronize on.
     *
     * @param cfMetaData Metadata of the column family in which these descriptors reside.
     * @param descriptors Set of SSTable descriptors to be atomically deleted. Guaranteed to be in the same columnfamily.
     *
     * @throws RuntimeException Signals that writing to the write-ahead log failed, and that none of the descriptors
     * should be deleted in this runtime.
     */
    void markForDeletion(CFMetaData cfMetaData, Set<Descriptor> descriptors);
}
