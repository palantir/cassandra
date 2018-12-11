/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.cassandra.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.tools.Util;

/**
 * Set partitioner on a given set of sstables.
 *
 * Example:
 *
 * {@code
 * sstablepartitionerset -p org.apache.cassandra.dht.ByteOrderedPartitioner -f <(find /var/lib/cassandra/data/.../ -iname "*Data.db*" -mtime +14)
 * }
 */
public class SSTablePartitionerSetter
{
    /**
     * @param args a list of sstables whose metadata we are changing
     */
    public static void main(final String[] args) throws IOException
    {
        PrintStream out = System.out;
        if (args.length == 0)
        {
            out.println("This command should be run with Cassandra stopped!");
            out.println("Usage: sstablepartitionerset -p <canonical-name-for-partitioner> [-f <sstable-list> | <sstables>]");
            System.exit(1);
        }

        if (args.length < 4 || !args[0].equals("--really-set") || !args[1].equals("-p"))
        {
            out.println("THIS IS AN EXTREMELY RISKY COMMAND TO RUN!!! YOU WILL HAVE DATA LOSS IF YOU DON'T UNDERSTAND WHAT YOU ARE DOING!!");
            out.println("THERE ARE VERY VERY FEW CASES WHERE THIS COMMAND IS ACTUALLY WHAT YOU WANT.");
            out.println("MORE CONCRETELY, THIS COMMAND WILL NOT DO ANYTHING TO ACTUALLY REDISTRIBUTE/REPARTITION YOUR DATA.\n");
            out.println("Verify that Cassandra is not running and then execute the command like this:");
            out.println("Usage: sstablepartitionerset --really-set -p <canonical-name-for-partitioner> [-f <sstable-list> | <sstables>]");
            System.exit(1);
        }

        Util.initDatabaseDescriptor();

        List<String> fileNames;
        if (args[3].equals("-f"))
        {
            fileNames = Files.readAllLines(Paths.get(args[4]), StandardCharsets.UTF_8);
        }
        else
        {
            fileNames = Arrays.asList(args).subList(3, args.length);
        }

        String partitioner = args[2];
        out.println("Setting partitioner to " + partitioner);
        for (String fname: fileNames)
        {
            Descriptor descriptor = Descriptor.fromFilename(fname);
            if (descriptor.version.isLatestVersion())
            {
                descriptor.getMetadataSerializer().mutatePartitioner(descriptor, partitioner);
            }
            else
            {
                System.err.println("SSTable " + fname + " is not on the latest version, run upgradesstables first");
                System.exit(1);
            }
        }
    }
}
