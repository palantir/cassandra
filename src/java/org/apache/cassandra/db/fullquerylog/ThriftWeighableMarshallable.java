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

package org.apache.cassandra.db.fullquerylog;

import java.io.ByteArrayOutputStream;

import com.palantir.cassandra.utils.ThriftUtils;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

public class ThriftWeighableMarshallable extends AbstractWeighableMarshallable
{

    private byte[] buffer;
    private String type;
    private long timestamp;
    private TBase thriftRequest;

    private static final String TYPE_FIELD = "type";
    private static final String QUERY_TIME_FIELD = "query-time";
    private static final String PAYLOAD_FIELD = "payload";

    public ThriftWeighableMarshallable() {
    }

    public ThriftWeighableMarshallable(TBase thriftRequest, String type, long timestamp) throws TException
    {
        this.thriftRequest = thriftRequest;
        this.buffer = ThriftUtils.toBytes(thriftRequest);
        this.type = type;
        this.timestamp = timestamp;
    }


    public static ThriftWeighableMarshallable create(TBase request, String type, long timestamp) throws TException
    {
        return new ThriftWeighableMarshallable(request, type, timestamp);
    }

    public void release()
    {
    }

    Method method()
    {
        return Method.THRIFT;
    }

    public void writeMarshallable(WireOut wire)
    {
        super.writeMarshallable(wire);
        wire.write(TYPE_FIELD).text(type);
        wire.write(QUERY_TIME_FIELD).int64(timestamp);
        wire.write(PAYLOAD_FIELD).bytes(buffer);
    }

    public void readMarshallable(WireIn wire)
    {
        // you must read and write in the same order, otherwise weird things happen
        type = wire.read(TYPE_FIELD).text();
        timestamp = wire.read(QUERY_TIME_FIELD).int64();
        buffer = wire.read(PAYLOAD_FIELD).bytes();
        ProcessFunction processFunction = ThriftUtils.THRIFT_PROCESS_MAP.get(type);
        if (processFunction == null) {
            throw new IORuntimeException("Cannot read thrift query of type " + type);
        }
        try
        {
            thriftRequest = processFunction.getEmptyArgsInstance();
            ThriftUtils.read(thriftRequest, buffer);
        }
        catch (TException e)
        {
            throw new IORuntimeException(e);
        }
    }

    public int weight()
    {
        return FullQueryLogger.OBJECT_HEADER_SIZE +
               Method.THRIFT.name().getBytes().length +
               TYPE_FIELD.getBytes().length +
               8 + // 8 bytes for timestamp
               buffer.length;
    }

    public byte[] getBuffer()
    {
        return buffer;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public String getType()
    {
        return type;
    }

    public TBase getThriftRequest()
    {
        return thriftRequest;
    }
}
