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
package org.apache.cassandra.db;

import java.io.*;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.filter.PageToken;
import org.apache.cassandra.db.filter.PageTokenDigest;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

/*
 * The read response message is sent by the server when reading data
 * this encapsulates the keyspacename and the row that has been read.
 * The keyspace name is needed so that we can use it to create repairs.
 */
public class ReadResponse
{
    public static final IVersionedSerializer<ReadResponse> serializer = new ReadResponseSerializer();

    private final Row row;
    private final PageToken pageToken;
    private volatile ByteBuffer digest;
    private volatile PageTokenDigest pageTokenDigest;
    // need to add page token here or somehow incorporate into the digest

    public ReadResponse(ByteBuffer digest, PageTokenDigest pageTokenDigest)
    {
        this(null, null, digest, pageTokenDigest);
        assert digest != null;
    }

    public ReadResponse(Row row, PageToken pageToken)
    {
        this(row, pageToken, null, null);
        assert row != null;
    }

    public ReadResponse(Row row, PageToken pageToken, ByteBuffer digest, PageTokenDigest pageTokenDigest)
    {
        this.row = row;
        this.pageToken = pageToken;
        this.digest = digest;
        this.pageTokenDigest = pageTokenDigest;
    }

    public Row row()
    {
        return row;
    }

    public PageToken pageToken()
    {
        return pageToken;
    }

    public ByteBuffer digest()
    {
        return digest;
    }

    public PageTokenDigest pageTokenDigest()
    {
        return pageTokenDigest;
    }

    public synchronized void setDigest(ByteBuffer digest, PageTokenDigest pageTokenDigest)
    {
        this.digest = digest;
        this.pageTokenDigest = pageTokenDigest;
    }

    public boolean isDigestQuery()
    {
        return digest != null && row == null;
    }
}

class ReadResponseSerializer implements IVersionedSerializer<ReadResponse>
{
    public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
    {
        out.writeInt(response.isDigestQuery() ? response.digest().remaining() : 0);
        ByteBuffer buffer = response.isDigestQuery() ? response.digest() : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        out.write(buffer);
        out.writeBoolean(response.isDigestQuery());
        if (response.isDigestQuery() && version >= MessagingService.VERSION_22_18)
        {
            PageTokenDigest pageTokenDigest = response.pageTokenDigest();
            boolean pageTokenDigestExists = pageTokenDigest != null;
            out.writeBoolean(pageTokenDigestExists);
            if (pageTokenDigestExists)
            {

            }
        }
        if (!response.isDigestQuery())
        {
            Row.serializer.serialize(response.row(), out, version);
            if (version >= MessagingService.VERSION_22_18)
            {
                PageToken pageToken = response.pageToken();
                boolean pageTokenDigestExists = pageToken != null;
                out.writeBoolean(pageTokenDigestExists);
                if (pageTokenDigestExists)
                {
                    new PageToken.Serializer(response.row().cf.getComparator().columnSerializer()).serialize(pageToken, out, version);
                }
            }
        }
    }

    public ReadResponse deserialize(DataInput in, int version) throws IOException
    {
        byte[] digest = null;
        int digestSize = in.readInt();
        if (digestSize > 0)
        {
            digest = new byte[digestSize];
            in.readFully(digest, 0, digestSize);
        }
        boolean isDigest = in.readBoolean();
        assert isDigest == digestSize > 0;

        Row row = null;
        if (!isDigest)
        {
            // This is coming from a remote host
            row = Row.serializer.deserialize(in, version, ColumnSerializer.Flag.FROM_REMOTE);
        }

        return isDigest ? new ReadResponse(ByteBuffer.wrap(digest)) : new ReadResponse(row);
    }

    public long serializedSize(ReadResponse response, int version)
    {
        TypeSizes typeSizes = TypeSizes.NATIVE;
        ByteBuffer buffer = response.isDigestQuery() ? response.digest() : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        int size = typeSizes.sizeof(buffer.remaining());
        size += buffer.remaining();
        size += typeSizes.sizeof(response.isDigestQuery());
        if (!response.isDigestQuery())
            size += Row.serializer.serializedSize(response.row(), version);
        return size;
    }
}
