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


public class ColumnFamilyStoreManager implements IColumnFamilyStoreValidator
{
    public static final ColumnFamilyStoreManager instance = new ColumnFamilyStoreManager();
    private final List<IColumnFamilyStoreValidator> validators;

    private ColumnFamilyStoreManager()
    {
        this.validators = new CopyOnWriteArrayList<>();
    }

    public void registerValidator(IColumnFamilyStoreValidator validator)
    {
        validators.add(validator);
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
}
