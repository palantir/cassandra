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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotificationConsumer;


public class ColumnFamilyStoreManager implements IColumnFamilyStoreValidator, SSTableDeletionTracker
{
    public static final ColumnFamilyStoreManager instance = new ColumnFamilyStoreManager();
    private final List<IColumnFamilyStoreValidator> validators;
    private final List<SSTableDeletionTracker> deletionTrackers;
    private final INotificationConsumer cfTrackerSubscriber;

    private ColumnFamilyStoreManager()
    {
        this.validators = new CopyOnWriteArrayList<>();
        this.deletionTrackers = new CopyOnWriteArrayList<>();
        this.cfTrackerSubscriber = new SSTableDeletionSubscriber();
    }

    public INotificationConsumer getCfTrackerSubscriber() {
        return cfTrackerSubscriber;
    }

    public void registerValidator(IColumnFamilyStoreValidator validator)
    {
        validators.add(validator);
    }

    public void unregisterValidator(IColumnFamilyStoreValidator validator)
    {
        validators.remove(validator);
    }

    public void completeSetup() {
        deletionTrackers.forEach(SSTableDeletionTracker::completeSetup);
    }

    public void registerDeletionTracker(SSTableDeletionTracker tracker)
    {
        deletionTrackers.add(tracker);
    }

    public void unregisterDeletionTracker(SSTableDeletionTracker tracker)
    {
        deletionTrackers.remove(tracker);
    }

    public Map<Descriptor, Set<Integer>> filterValidAncestors(CFMetaData cfMetaData, Map<Descriptor, Set<Integer>> sstableToCompletedAncestors, Map<Integer, UUID> unfinishedCompactions)
    {
        Map<Descriptor, Set<Integer>> filtered = sstableToCompletedAncestors;
        for (IColumnFamilyStoreValidator validator : validators)
        {
            filtered = validator.filterValidAncestors(cfMetaData, filtered, unfinishedCompactions);
        }
        return filtered;
    }

    /** All registered validators agree this SSTable is obsolete. If no validators are registered, returns true. */
    public boolean isObsolete(Descriptor desc)
    {
        return deletionTrackers.stream().allMatch(tracker -> tracker.isObsolete(desc));
    }

    public void markObsoleted(SSTableReader deleting)
    {
        deletionTrackers.forEach(tracker -> tracker.markObsoleted(deleting));
    }
}
