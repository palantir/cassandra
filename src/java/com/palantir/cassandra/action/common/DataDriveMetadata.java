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

package com.palantir.cassandra.action.common;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.FBUtilities;

public final class DataDriveMetadata
{
    private static final String KEYSPACE = "system_palantir_local";

    private static final String TABLE_NAME = "cassandra_data_drive_metadata";

    private static final CFMetaData TABLE = CFMetaData.compile(
                                                      "CREATE TABLE %s ("
                                                      + "host_id uuid,"
                                                      + "written_at timestamp,"
                                                      + "PRIMARY KEY host_id", TABLE_NAME)
                                                      .comment("Metadata for the data volume");

    public final UUID hostId;

    public final Instant writtenAt;

    public DataDriveMetadata(UUID hostId, Instant writtenAt)
    {
        this.hostId = hostId;
        this.writtenAt = writtenAt;
    }

    public static synchronized Optional<DataDriveMetadata> readFromFile(UUID hostId)
    {
        String query = String.format("SELECT * FROM %s.%s WHERE host_id='%s'", KEYSPACE, TABLE_NAME, hostId);

        Optional<UntypedResultSet> maybeResult = Optional.ofNullable(QueryProcessor.executeInternal(query));
        return maybeResult
               .map(UntypedResultSet::one)
               .map(row -> new DataDriveMetadata(row.getUUID("host_id"),
                                                 row.getTimestamp("written_at").toInstant()));
    }

    public static synchronized void writeToFile(UUID hostId)
    {
        String query = String.format("INSERT INTO %s.%s (host_id, written_at) " +
                                     "VALUES (%s, %s)" +
                                     "IF NOT EXISTS",
                                     KEYSPACE,
                                     TABLE_NAME,
                                     hostId,
                                     Instant.now());
        QueryProcessor.executeOnceInternal(query);
        FBUtilities.waitOnFuture(Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE_NAME).forceFlush("Persist drive metadata"));
    }
}
