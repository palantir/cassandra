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

package com.palantir.cassandra.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

import com.palantir.cassandra.thrift.DummyCassandra;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryBuffer;

public class ThriftUtils
{
    public static final Map<String, ProcessFunction<Cassandra.Client, ? extends  TBase>> THRIFT_PROCESS_MAP = new Cassandra.Processor(new DummyCassandra()).getProcessMapView();

    @SuppressWarnings({ "resource"})
    public static final void read(TBase base, byte[] value) throws TException
    {
        // No need to close, it's a NO-OP
        base.read(new TBinaryProtocol(new TIOStreamTransport(new ByteArrayInputStream(value))));
    }

    @SuppressWarnings({"resource"})
    public static final byte[] toBytes(TBase request) throws TException
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.write(new TBinaryProtocol(new TIOStreamTransport(outputStream)));
        return outputStream.toByteArray();
    }
}
