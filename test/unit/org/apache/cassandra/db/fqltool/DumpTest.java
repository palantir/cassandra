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

package org.apache.cassandra.db.fqltool;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.palantir.cassandra.utils.ThriftUtils;
import net.jpountz.util.ByteBufferUtils;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.fullquerylog.FullQueryLogger;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.tools.fqltool.Dump;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.binlog.BinLogTest;
import org.apache.thrift.TException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DumpTest
{

    private static Path tempDir;
    private static String ROLL_CYCLE = "TEST_SECONDLY";


    @BeforeClass
    public static void beforeClass() throws Exception
    {
        tempDir = BinLogTest.tempDir();
    }

    @After
    public void tearDown()
    {
        FullQueryLogger.instance.reset(tempDir.toString());
    }

    @Test
    public void canReadThriftAndCqlMessages() throws Exception
    {
        configureFQL();
        long timestamp = System.currentTimeMillis();
        Cassandra.multiget_slice_args args = create(ImmutableList.of(ByteBufferUtil.bytes("a")),
                                                    ImmutableList.of(ByteBufferUtil.bytes("b")),
                                                    ByteBufferUtil.bytes("1"),
                                                    ByteBufferUtil.bytes("10"),
                                                    new ColumnParent("test"));

        FullQueryLogger.instance.logThrift(args, "multiget_slice", timestamp);
        FullQueryLogger.instance.logThrift(args, "multiget_slice", timestamp);
        FullQueryLogger.instance.logQuery("foo", QueryOptions.DEFAULT, timestamp);
        FullQueryLogger.instance.logThrift(args, "multiget_slice", timestamp);

        Util.spinAssertEquals(true, () ->
        {
            try (ChronicleQueue queue = ChronicleQueueBuilder.single(tempDir.toFile()).rollCycle(RollCycles.TEST_SECONDLY).build())
            {
                return queue.createTailer().readingDocument().isPresent();
            }
        }, 60);
        Dump.dump(ImmutableList.of(tempDir.toString()), ROLL_CYCLE, false);
    }
    private Cassandra.multiget_slice_args create(List<ByteBuffer> keys, List<ByteBuffer> columns, ByteBuffer rangeStart, ByteBuffer rangeEnd, ColumnParent columnParent) {
        String type = "multiget_slice";
        SliceRange sliceRange = new SliceRange(rangeStart, rangeEnd, false, 10);
        SlicePredicate slicePredicate = new SlicePredicate();
        slicePredicate.setColumn_names(columns);
        slicePredicate.setSlice_range(sliceRange);
        ConsistencyLevel consistencyLevel = ConsistencyLevel.QUORUM;
        return new Cassandra.multiget_slice_args(keys, columnParent, slicePredicate, consistencyLevel);
    }

    private void configureFQL() throws Exception
    {
        FullQueryLogger.instance.configure(tempDir, ROLL_CYCLE, true, 1, 1024 * 1024 * 256);
    }
}
