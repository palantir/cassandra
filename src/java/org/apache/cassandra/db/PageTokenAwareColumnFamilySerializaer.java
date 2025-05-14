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

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.db.filter.PageToken;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

public class PageTokenAwareColumnFamilySerializaer extends ColumnFamilySerializer
{
    /*
     * <page token set boolean>
     * <page token if set, serialized using the same serializer as the columns>
     */
    public void serialize(ColumnFamily cf, DataOutputPlus out, int version)
    {
        super.serialize(cf, out, version);
        try
        {
            if (version >= MessagingService.VERSION_22_PLTR)
            {
                ColumnSerializer columnSerializer = cf.getComparator().columnSerializer();
                out.writeBoolean(cf.isPageTokenSet());
                if (cf.isPageTokenSet())
                {
                    new PageToken.Serializer(columnSerializer).serialize(cf.pageToken(), out, version);
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public ColumnFamily deserialize(DataInput in, ColumnFamily.Factory factory, ColumnSerializer.Flag flag, int version) throws IOException
    {
        ColumnFamily cf = super.deserialize(in, factory, flag, version);
        if (version >= MessagingService.VERSION_22_PLTR)
        {
            boolean isPageTokenSet = in.readBoolean();
            if (isPageTokenSet)
            {
                ColumnSerializer columnSerializer = cf.getComparator().columnSerializer();
                PageToken pageToken = new PageToken.Serializer(columnSerializer).deserialize(in, flag, version);
                if (pageToken.isReachedEnd())
                {
                    cf.setPageTokenEndOfRow();
                }
                else
                {
                    cf.setPageToken(pageToken.getCell());
                }
            }
        }
        return cf;
    }

    public long serializedSize(ColumnFamily cf, TypeSizes typeSizes, int version)
    {
        if (version < MessagingService.VERSION_22_PLTR)
        {
            return 0;
        }

        long size = typeSizes.sizeof(cf.isPageTokenSet());
        if (cf.isPageTokenSet())
        {
            size += new PageToken.Serializer(cf.getComparator().columnSerializer()).serializedSize(cf.pageToken(), typeSizes, version);
        }
        return size;
    }
}
