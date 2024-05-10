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
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;
import org.apache.commons.cli.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Export SSTables to JSON format.
 */
public class SSTableSQLExport
{
    public static void main(String[] args) throws ConfigurationException, IOException
    {
        if (args.length < 2)
        {
            System.err.println("Usage: ./sstable2sql keyspace table");
            System.exit(1);
        }

        String keyspaceName = args[0];
        String tableName = args[1];

        DatabaseDescriptor.setDaemonInitialized();
        Keyspace.setInitialized();

//        if (!Gossiper.instance.isEnabled())
//        {
//            Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
//        }

        MigrationManager.announceNewKeyspace(
                new KSMetaData(keyspaceName,
                        LocalStrategy.class,
                        Collections.<String, String>emptyMap(),
                        true,
                        ImmutableList.of(CFMetaData.compile(
                                "CREATE TABLE " + tableName + " (\n" +
                                        "    key blob,\n" +
                                        "    column1 blob,\n" +
                                        "    column2 bigint,\n" +
                                        "    value blob,\n" +
                                        "    PRIMARY KEY (key, column1, column2)\n" +
                                        ") WITH COMPACT STORAGE\n" +
                                        "    AND CLUSTERING ORDER BY (column1 ASC, column2 ASC)\n" +
                                        "    AND bloom_filter_fp_chance = 0.1\n" +
                                        "    AND caching = '{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}'\n" +
                                        "    AND comment = ''\n" +
                                        "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}\n" +
                                        "    AND compression = {'chunk_length_kb': '8', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
                                        "    AND dclocal_read_repair_chance = 0.0\n" +
                                        "    AND default_time_to_live = 0\n" +
                                        "    AND gc_grace_seconds = 3600\n" +
                                        "    AND max_index_interval = 2048\n" +
                                        "    AND memtable_flush_period_in_ms = 0\n" +
                                        "    AND min_index_interval = 128\n" +
                                        "    AND read_repair_chance = 0.0\n" +
                                        "    AND speculative_retry = 'NONE';",
                                keyspaceName))),
                true);

//        Schema.instance.loadFromDisk(false);

//        System.exit(0);

        System.err.println("Known keyspaces:");
        Schema.instance.getKeyspaces().forEach(keyspace -> System.err.println("\t" + keyspace));

        if (Schema.instance.getKSMetaData(keyspaceName) == null)
        {
            throw new IllegalArgumentException(String.format("Unknown keyspace %s", keyspaceName));
        }

        System.err.println("Opening keyspace " + keyspaceName + "...");
        Keyspace keyspace = Keyspace.openWithoutSSTables(keyspaceName);
        Iterable<ColumnFamilyStore> knownTables = keyspace.getValidColumnFamilies(true, false);
        System.err.println("Known tables in keyspace " + keyspaceName + ":");
        knownTables.forEach(knownTable -> System.err.println("\t" + knownTable.name));
        ColumnFamilyStore cfs = StreamSupport.stream(knownTables.spliterator(), false)
                .filter(knownTable -> knownTable.name.equals(tableName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Unknown table %s.%s",
                        keyspaceName,
                        tableName)));

        System.err.println();
        System.err.println("SSTables for table " + keyspaceName + "." + tableName + ":");
        Map<Descriptor, Set<Component>> sstableComponents = cfs.directories.sstableLister().skipTemporary(true).list();
        sstableComponents.forEach((descriptor, components) -> System.err.println("\t" + descriptor + " (" + components + ")"));
        sstableComponents.forEach((descriptor, components) -> {
            try
            {
                SSTableReader reader = SSTableReader.openNoValidation(descriptor, components, cfs);
                SSTableExport.export(reader, System.out, new String [] {});
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });

        // SSTableExport.export(reader, System.out, new String[] {});

        System.exit(0);
    }
}
