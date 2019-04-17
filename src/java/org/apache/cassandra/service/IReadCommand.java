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
package org.apache.cassandra.service;

public interface IReadCommand
{
    /**
     * Palantir addition to enable better scheduling.
     * <p>
     * Cassandra services are typically designed to service individual workflows, so the read commands executed are
     * all roughly the same cost. This means that a FIFO queue (what is used) seems sensible. Unfortunately our reads
     * are very different in size, and so we see very variable properties wrt latency. Thus, this code is to
     * experiment with splitting workloads across different executors by whether we can guess that they're cheap or not.
     */
    public boolean isCheap();
    public String getKeyspace();
    public long getTimeout();
}
