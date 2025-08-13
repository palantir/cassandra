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

package com.palantir.cassandra.logicalts;

public interface FrozenTimestampTracker
{
    /**
     * Advance the frozenTimestamp to the desiredTimestamp. This method throws
     * if there are in-progress mutations holding a lock at or before the desiredTimestamp.
     * If the implementation persists the frozenTimestamp, the timestamp
     * must be persisted durably before this method returns.
     *
     * @param desiredTimestamp An observed Sweep timestamp
     * @throws InProgressMutationConflictException if there are in-progress mutations holding a lock at or before the given timestamp
     */
    void advanceFrozenTimestamp(long desiredTimestamp) throws InProgressMutationConflictException;

    /**
     * Lock a Mutation timestamp. This lock must be held while applying a Mutation.
     *
     * @param mutationTimestamp The write time of the Mutation. We will hold a lock on this timestamp,
     *                          and the frozen timestamp will not be allowed to increase past this
     *                          timestamp while we hold this lock.
     * @return An AutoClosable that releases the lock on the mutationTimestamp when closed
     * @throws IllegalLogicalTimestampException if the provided startTimestamp < the current Frozen Timestamp
     */
    UncheckedAutoCloseable checkAndLockForMutation(long mutationTimestamp) throws IllegalLogicalTimestampException;
}
