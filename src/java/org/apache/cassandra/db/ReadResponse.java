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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.cassandra.db.filter.PageTokenDigest;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;

/*
 * The read response message is sent by the server when reading data
 * this encapsulates the keyspacename and the row that has been read.
 * The keyspace name is needed so that we can use it to create repairs.
 */
public class ReadResponse
{
    public static final IVersionedSerializer<ReadResponse> serializer = new ReadResponseSerializer();
    private static final AtomicReferenceFieldUpdater<ReadResponse, Pair<ByteBuffer, PageTokenDigest>> digestUpdater =
            AtomicReferenceFieldUpdater.newUpdater(ReadResponse.class, (Class<Pair<ByteBuffer, PageTokenDigest>>) (Class<?>) Pair.class, "digest");

    private final Row row;
    private volatile Pair<ByteBuffer, PageTokenDigest> digest;

    public ReadResponse(ByteBuffer dataDigest, PageTokenDigest pageTokenDigest)
    {
        this(null, dataDigest, pageTokenDigest);
        Throwables.assertWithError(dataDigest != null);
    }

    public ReadResponse(Row row)
    {
        this(row, null, null);
        assert row != null;
    }

    private ReadResponse(Row row, ByteBuffer dataDigest, PageTokenDigest pageTokenDigest)
    {
        this.row = row;
        this.digest = Pair.create(dataDigest, pageTokenDigest);
    }

    public Row row()
    {
        return row;
    }

    public ByteBuffer digest()
    {
        return digest.left;
    }

    public PageTokenDigest pageTokenDigest()
    {
        return digest.right;
    }

    public void setDigest(ByteBuffer dataDigest, PageTokenDigest pageTokenDigest)
    {
        Pair<ByteBuffer, PageTokenDigest> curr = this.digest;
        Pair<ByteBuffer, PageTokenDigest> newDigest = Pair.create(dataDigest, pageTokenDigest);
        if (!digestUpdater.compareAndSet(this, curr, newDigest))
        {
            Throwables.assertWithError(newDigest.equals(this.digest),
                    String.format("Digest mismatch : data(%s), pageToken(%s) vs data(%s), pageTokenDigest(%s)",
                            Arrays.toString(dataDigest.array()),
                            pageTokenDigest,
                            Arrays.toString(this.digest.left.array()),
                            this.digest.right));
        }
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
        if (response.isDigestQuery() && version >= MessagingService.VERSION_22_PLTR)
        {
            PageTokenDigest pageTokenDigest = response.pageTokenDigest();
            boolean pageTokenDigestExists = pageTokenDigest != null;
            out.writeBoolean(pageTokenDigestExists);
            if (pageTokenDigestExists)
            {
                PageTokenDigest.serializer.serialize(pageTokenDigest, out, version);
            }
        }
        if (!response.isDigestQuery())
        {
            Row.serializer.serialize(response.row(), out, version);
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

        if (isDigest)
        {
            if (version < MessagingService.VERSION_22_PLTR)
            {
                return new ReadResponse(ByteBuffer.wrap(digest), null);
            }
            boolean pageTokenDigestExists = in.readBoolean();
            if (pageTokenDigestExists)
            {
                PageTokenDigest pageTokenDigest = PageTokenDigest.serializer.deserialize(in, version);
                return new ReadResponse(ByteBuffer.wrap(digest), pageTokenDigest);
            }
            return new ReadResponse(ByteBuffer.wrap(digest), null);
        }

        // This is coming from a remote host
        Row row = Row.serializer.deserialize(in, version, ColumnSerializer.Flag.FROM_REMOTE);
        return new ReadResponse(row);
    }

    public long serializedSize(ReadResponse response, int version)
    {
        TypeSizes typeSizes = TypeSizes.NATIVE;
        ByteBuffer buffer = response.isDigestQuery() ? response.digest() : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        int size = typeSizes.sizeof(buffer.remaining());
        size += buffer.remaining();
        size += typeSizes.sizeof(response.isDigestQuery());
        if (response.isDigestQuery() && version >= MessagingService.VERSION_22_PLTR)
        {
            boolean pageTokenDigestExists = response.pageTokenDigest() != null;
            size += typeSizes.sizeof(pageTokenDigestExists);
            if (pageTokenDigestExists)
            {
                size += PageTokenDigest.serializer.serializedSize(response.pageTokenDigest(), version);
            }
        }
        if (!response.isDigestQuery())
        {
            size += Row.serializer.serializedSize(response.row(), version);
        }

        return size;
    }
}
