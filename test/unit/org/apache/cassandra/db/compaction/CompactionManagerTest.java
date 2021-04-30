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

package org.apache.cassandra.db.compaction;

import org.junit.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class CompactionManagerTest
{
    @Test
    public void stopAllCompactions_stopsInProgressCompactions() {
        CompactionManager manager = spy(CompactionManager.instance);
        manager.stopAllCompactions();
        for (OperationType type : OperationType.values()) {
            if (CompactionManager.STOPPABLE_COMPACTION_TYPES.contains(type)) {
                verify(manager).stopCompaction(eq(type.name()));
            } else {
                verify(manager, never()).stopCompaction(eq(type.name()));
            }
        }
    }
}
