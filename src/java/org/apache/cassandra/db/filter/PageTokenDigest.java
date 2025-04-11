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

package org.apache.cassandra.db.filter;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PageTokenDigest
{
    public static final PageTokenDigestSerializer serializer = new PageTokenDigestSerializer();

    private final ByteBuffer digest;
    private final boolean reachedEnd;

    private PageTokenDigest(ByteBuffer digest, boolean reachedEnd)
    {
        this.digest = digest;
        this.reachedEnd = reachedEnd;
    }

    public ByteBuffer digest()
    {
        return digest;
    }

    public boolean isReachedEnd()
    {
        return reachedEnd;
    }

    public static PageTokenDigest createPageTokenDigest(ByteBuffer digest)
    {
        return new PageTokenDigest(digest, false);
    }

    public static PageTokenDigest createPageTokenReachedEnd()
    {
        return new PageTokenDigest(null, true);
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof PageTokenDigest && equals((PageTokenDigest) o));
    }

    private boolean equals(PageTokenDigest pageTokenDigest)
    {
        return Objects.equals(digest, pageTokenDigest.digest) && reachedEnd == pageTokenDigest.reachedEnd;
    }

    @Override
    public String toString()
    {
        if (reachedEnd)
        {
            return "EndOfRowDigest";
        }
        else
        {
            return "PageTokenDigest(" + ByteBufferUtil.bytesToHex(digest) + ")";
        }
    }

    public static class PageTokenDigestSerializer implements IVersionedSerializer<PageTokenDigest>
    {
        @Override
        public void serialize(PageTokenDigest pageTokenDigest, DataOutputPlus out, int version) throws IOException
        {
            assert version >= MessagingService.VERSION_22_PLTR;

            boolean hasReachedEnd = pageTokenDigest.isReachedEnd();
            out.writeBoolean(hasReachedEnd);
            if (!hasReachedEnd)
            {
                int digestSize = pageTokenDigest.digest().remaining();
                assert digestSize > 0;
                out.writeInt(digestSize);
                out.write(pageTokenDigest.digest());
            }
        }

        @Override
        public PageTokenDigest deserialize(DataInput in, int version) throws IOException
        {
            assert version >= MessagingService.VERSION_22_PLTR;

            boolean hasReachedEnd = in.readBoolean();
            if (!hasReachedEnd)
            {
                int digestSize = in.readInt();
                assert digestSize > 0;
                byte[] buffer = new byte[digestSize];
                in.readFully(buffer, 0, digestSize);
                return PageTokenDigest.createPageTokenDigest(ByteBuffer.wrap(buffer));
            }
            return PageTokenDigest.createPageTokenReachedEnd();
        }

        @Override
        public long serializedSize(PageTokenDigest pageTokenDigest, int version)
        {
            assert version >= MessagingService.VERSION_22_PLTR;
            
            TypeSizes typeSizes = TypeSizes.NATIVE;

            boolean hasReachedEnd = pageTokenDigest.isReachedEnd();
            int size = typeSizes.sizeof(hasReachedEnd);
            if (!hasReachedEnd)
            {
                ByteBuffer buffer = pageTokenDigest.digest();
                size += typeSizes.sizeof(buffer.remaining());
                size += buffer.remaining();
            }
            return size;
        }
    }
}
