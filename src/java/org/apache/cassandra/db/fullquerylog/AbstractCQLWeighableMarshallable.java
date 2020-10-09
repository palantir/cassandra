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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.transport.CBUtil;

public abstract class AbstractCQLWeighableMarshallable extends AbstractWeighableMarshallable
{
    private ByteBuf queryOptionsBuffer;
    private long timeMillis;
    private int protocolVersion;

    protected  AbstractCQLWeighableMarshallable() {
    }

    protected AbstractCQLWeighableMarshallable(QueryOptions queryOptions, long timeMillis)
    {
        this.timeMillis = timeMillis;
        this.protocolVersion = queryOptions.getProtocolVersion();
        int optionsSize = QueryOptions.codec.encodedSize(queryOptions, protocolVersion);
        queryOptionsBuffer = CBUtil.allocator.buffer(optionsSize, optionsSize);
        /*
         * Struggled with what tradeoff to make in terms of query options which is potentially large and complicated
         * There is tension between low garbage production (or allocator overhead), small working set size, and CPU overhead reserializing the
         * query options into binary format.
         *
         * I went with the lowest risk most predictable option which is allocator overhead and CPU overhead
         * rather then keep the original query message around so I could just serialize that as a memcpy. It's more
         * instructions when turned on, but it doesn't change memory footprint quite as much and it's more pay for what you use
         * in terms of query volume. The CPU overhead is spread out across producers so we should at least get
         * some scaling.
         *
         */
        boolean success = false;
        try
        {
            QueryOptions.codec.encode(queryOptions, queryOptionsBuffer, protocolVersion);
            success = true;
        }
        finally
        {
            if (!success)
            {
                queryOptionsBuffer.release();
            }
        }
    }

    abstract String type();

    @Override
    public void readMarshallable(WireIn wire) {
        protocolVersion = wire.read("protocol-version").int32();
        if (queryOptionsBuffer != null) {
            queryOptionsBuffer.release();
        }
        queryOptionsBuffer = Unpooled.wrappedBuffer(wire.read("query-options").bytesStore().toTemporaryDirectByteBuffer());
        timeMillis = wire.read("query-time").int64();
    }

    @Override
    public void writeMarshallable(WireOut wire)
    {
        super.writeMarshallable(wire);
        wire.write("type").text(type());
        wire.write("protocol-version").int32(protocolVersion);
        wire.write("query-options").bytes(BytesStore.wrap(queryOptionsBuffer.nioBuffer()));
        wire.write("query-time").int64(timeMillis);
    }

    @Override
    public void release()
    {
        queryOptionsBuffer.release();
    }

    //3-bytes for method type
    //8-bytes for protocol version (assume alignment cost), 8-byte timestamp, 8-byte object header + other contents
    @Override
    public int weight()
    {
        return 3 + 8 + 8 + FullQueryLogger.OBJECT_HEADER_SIZE + FullQueryLogger.EMPTY_BYTEBUF_SIZE + queryOptionsBuffer.capacity();
    }

    public Method method() {
        return Method.CQL;
    }

    public static String readType(WireIn wire) {
        return wire.read("type").text();
    }
}
