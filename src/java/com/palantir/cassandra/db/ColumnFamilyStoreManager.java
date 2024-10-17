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
    public static final ColumnFamilyStoreManager instance = new ColumnFamilyStoreManager();
    private final List<IColumnFamilyStoreValidator> validators;
    private final List<IColumnFamilyStoreWriteAheadLogger> writeAheadLoggers;

    private ColumnFamilyStoreManager()
    {
        this.validators = new CopyOnWriteArrayList<>();
        this.writeAheadLoggers = new CopyOnWriteArrayList<>();
    }

    public void registerValidator(IColumnFamilyStoreValidator validator)
    {
        validators.add(validator);
    }

    public void unregisterValidator(IColumnFamilyStoreValidator validator)
    {
        validators.remove(validator);
    }

    public void registerWriteAheadLogger(IColumnFamilyStoreWriteAheadLogger writeAheadLogger) {
        writeAheadLoggers.add(writeAheadLogger);
    }

    public void unregisterWriteAheadLogger(IColumnFamilyStoreWriteAheadLogger writeAheadLogger) {
        writeAheadLoggers.remove(writeAheadLogger);
    }

    @Override
    public Map<Descriptor, Set<Integer>> filterValidAncestors(CFMetaData cfMetaData, Map<Descriptor, Set<Integer>> sstableToCompletedAncestors, Map<Integer, UUID> unfinishedCompactions)
    {
        Map<Descriptor, Set<Integer>> filtered = sstableToCompletedAncestors;
        for (IColumnFamilyStoreValidator validator : validators)
        {
            filtered = validator.filterValidAncestors(cfMetaData, filtered, unfinishedCompactions);
        }
        return filtered;
    }

    @Override
    public boolean shouldRemoveUnusedSstables() {
        return validators.stream()
            .map(IColumnFamilyStoreValidator::shouldRemoveUnusedSstables)
            .reduce(Boolean::logicalOr)
            .orElse(true);
    }

    @Override
    public void markForDeletion(CFMetaData cfMetaData, Set<Descriptor> descriptors)
    {
        writeAheadLoggers.forEach(l -> l.markForDeletion(cfMetaData, descriptors));
    }
}
