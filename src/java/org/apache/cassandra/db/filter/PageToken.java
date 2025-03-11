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

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class PageToken
{
    private final Cell pageToken;
    private final boolean reachedEnd;

    private PageToken(Cell pageToken, boolean reachedEnd)
    {
        this.pageToken = pageToken;
        this.reachedEnd = reachedEnd;
    }

    public Cell getPageToken()
    {
        return pageToken;
    }

    public boolean isReachedEnd()
    {
        return reachedEnd;
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

        public void serialize(PageToken pagetoken, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(pagetoken.reachedEnd);
            if (!pagetoken.reachedEnd)
            {
                columnSerializer.serialize(pagetoken.pageToken, out);
            }
        }

        public PageToken deserialize(DataInput in, int version) throws IOException
        {
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

        public long serializedSize(PageToken pageToken, int version)
        {
            return serializedSize(pageToken, TypeSizes.NATIVE, version);
        }

        public long serializedSize(PageToken pagetoken, TypeSizes typeSizes, int version)
        {
            long size = typeSizes.sizeof(pagetoken.reachedEnd);
            if (!pagetoken.reachedEnd)
            {
                size += columnSerializer.serializedSize(pagetoken.pageToken, typeSizes);
            }
            return size;
        }
    }
}
