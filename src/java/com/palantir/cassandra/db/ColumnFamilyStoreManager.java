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

package com.palantir.cassandra.db;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import com.palantir.cassandra.db.compaction.IColumnFamilyStoreWriteAheadLogger;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.Pair;


public class ColumnFamilyStoreManager implements IColumnFamilyStoreValidator, IColumnFamilyStoreWriteAheadLogger
{
    private static final IColumnFamilyStoreValidator NO_OP_VALIDATOR = (_cfMetaData, sstableToCompletedAncestors, _unfinishedCompactions) -> sstableToCompletedAncestors;
    private static final IColumnFamilyStoreWriteAheadLogger NO_OP_WRITE_AHEAD_LOGGER = (cfMetaData, descriptors) -> {};

    public static final ColumnFamilyStoreManager instance = new ColumnFamilyStoreManager();
    private volatile IColumnFamilyStoreValidator validator = NO_OP_VALIDATOR;
    private volatile IColumnFamilyStoreWriteAheadLogger writeAheadLogger = NO_OP_WRITE_AHEAD_LOGGER;

    private ColumnFamilyStoreManager() {}

    public void registerValidator(IColumnFamilyStoreValidator validator)
    {
        this.validator = validator;
    }

    public void unregisterValidator()
    {
        this.validator = NO_OP_VALIDATOR;
    }

    public void registerWriteAheadLogger(IColumnFamilyStoreWriteAheadLogger writeAheadLogger) {
        this.writeAheadLogger = writeAheadLogger;
    }

    public void unregisterWriteAheadLogger() {
        this.writeAheadLogger = NO_OP_WRITE_AHEAD_LOGGER;
    }

    @Override
    public Map<Descriptor, Set<Integer>> filterValidAncestors(CFMetaData cfMetaData, Map<Descriptor, Set<Integer>> sstableToCompletedAncestors, Map<Integer, UUID> unfinishedCompactions)
    {
        return validator.filterValidAncestors(cfMetaData, sstableToCompletedAncestors, unfinishedCompactions);
    }

    @Override
    public boolean shouldRemoveUnusedSstables() {
        return validator.shouldRemoveUnusedSstables();
    }

    @Override
    public boolean shouldSkipAncestorCleanup() {
        return validator.shouldSkipAncestorCleanup();
    }

    @Override
    public void markForDeletion(CFMetaData cfMetaData, Set<Descriptor> descriptors)
    {
        writeAheadLogger.markForDeletion(cfMetaData, descriptors);
    }
}
