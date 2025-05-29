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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;

public class PageToken
{
    private final Cell cell;
    private final boolean reachedEnd;

    private PageToken(Cell cell, boolean reachedEnd)
    {
        this.cell = cell;
        this.reachedEnd = reachedEnd;
    }

    public Cell getCell()
    {
        return cell;
    }

    public boolean isReachedEnd()
    {
        return reachedEnd;
    }

    public PageTokenDigest digest()
    {
        if (reachedEnd)
        {
            return PageTokenDigest.createPageTokenReachedEnd();
        }
        MessageDigest digest = FBUtilities.threadLocalMD5Digest();
        cell.updateDigest(digest);
        return PageTokenDigest.createPageTokenDigest(ByteBuffer.wrap(digest.digest()));
    }

    public static PageToken createPageToken(Cell pageToken)
    {
        return new PageToken(pageToken, false);
    }

    public static PageToken createPageTokenReachedEnd()
    {
        return new PageToken(null, true);
    }

    public static class Serializer implements IVersionedSerializer<PageToken>
    {
        ColumnSerializer columnSerializer;

        public Serializer(ColumnSerializer columnSerializer)
        {
            this.columnSerializer = columnSerializer;
        }

        @Override
        public void serialize(PageToken pagetoken, DataOutputPlus out, int version) throws IOException
        {
            Throwables.assertWithError(version >= MessagingService.VERSION_22_PLTR);

            out.writeBoolean(pagetoken.reachedEnd);
            if (!pagetoken.reachedEnd)
            {
                columnSerializer.serialize(pagetoken.cell, out);
            }
        }

        @Override
        public PageToken deserialize(DataInput in, int version) throws IOException
        {
            Throwables.assertWithError(version >= MessagingService.VERSION_22_PLTR);

            return deserialize(in, ColumnSerializer.Flag.LOCAL, version);
        }

        public PageToken deserialize(DataInput in, ColumnSerializer.Flag flag, int version) throws IOException
        {
            boolean reachedEnd = in.readBoolean();
            if (reachedEnd)
            {
                return PageToken.createPageTokenReachedEnd();
            }
            return PageToken.createPageToken(columnSerializer.deserialize(in, flag));
        }

        @Override
        public long serializedSize(PageToken pageToken, int version)
        {
            Throwables.assertWithError(version >= MessagingService.VERSION_22_PLTR);

            return serializedSize(pageToken, TypeSizes.NATIVE, version);
        }

        public long serializedSize(PageToken pagetoken, TypeSizes typeSizes, int version)
        {
            Throwables.assertWithError(version >= MessagingService.VERSION_22_PLTR);

            long size = typeSizes.sizeof(pagetoken.reachedEnd);
            if (!pagetoken.reachedEnd)
            {
                size += columnSerializer.serializedSize(pagetoken.cell, typeSizes);
            }
            return size;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof PageToken && equals((PageToken) o));
    }

    private boolean equals(PageToken pageToken)
    {
        return equals(cell, pageToken.cell) && reachedEnd == pageToken.reachedEnd;
    }

    private static boolean equals(Cell token1, Cell token2)
    {
        return (token1 == null && token2 == null) || (token1 != null && token1.equals(token2));
    }

    @Override
    public String toString()
    {
        if (reachedEnd)
        {
            return "EndOfRow";
        }
        else
        {
            return "Cell(" + cell.name().toString() + ")";
        }
    }

    public static class Comparator implements java.util.Comparator<PageToken>
    {
        private final CellNameType cellComparator;

        public Comparator(CellNameType cellComparator)
        {
            this.cellComparator = cellComparator;
        }

        @Override
        public int compare(PageToken pageToken1, PageToken pageToken2)
        {
            if (pageToken1.isReachedEnd() && pageToken2.isReachedEnd())
            {
                return 0;
            }
            else if (pageToken2.isReachedEnd())
            {
                return -1;
            }
            else if (pageToken1.isReachedEnd())
            {
                return 1;
            }
            else
            {
                return cellComparator.compare(pageToken1.getCell().name(), pageToken2.getCell().name());
            }
        }
    }
}
