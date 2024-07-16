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

package org.apache.cassandra.net;

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.ByteBufferDataInput;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.transport.Message;
import org.apache.hadoop.io.DataInputBuffer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;

public class MessageInTest
{
    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.setDaemonInitialized();
    }

    // make sure deserializing message doesn't crash with an unknown verb
    @Ignore("Not fixed in C* 2.x")
    @Test
    public void read_NullVerb() throws IOException
    {
        read(Integer.MAX_VALUE);
    }

    @Test
    public void read_NoSerializer() throws IOException
    {
        read(MessagingService.Verb.UNUSED_3.ordinal());
    }

    private void read(int verbOrdinal) throws IOException
    {
        InetAddress addr = InetAddress.getByName("127.0.0.1");
        int payloadSize = 27;
        ByteBuffer buffer = ByteBuffer.allocate(64);
        DataOutputBuffer out = new DataOutputBuffer(64);
        out.writeByte(addr.getAddress().length);
        out.write(addr.getAddress());
        out.writeInt(verbOrdinal);
        out.writeInt(0);
        out.writeInt(payloadSize);

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(out.buffer().array());
        DataInputStream dataInput = new DataInputStream(byteArrayInputStream);

        buffer.flip();

        Assert.assertEquals(0, buffer.position());
        Assert.assertNotNull(MessageIn.read(dataInput, 1, 42));
        // addr length, addr, verb, params, payload size, skipped payload
        Assert.assertEquals(64 - (1 + 4 + 4 + 4 + 4 + 27), dataInput.available());
    }
}