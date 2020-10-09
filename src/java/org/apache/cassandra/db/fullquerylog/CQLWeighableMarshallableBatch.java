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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.math.IntRange;

import io.netty.buffer.ByteBuf;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.utils.ObjectSizes;

public class CQLWeighableMarshallableBatch extends AbstractCQLWeighableMarshallable
{
    private int weight;
    private String batchType;
    private List<String> queries;
    private List<List<ByteBuffer>> values;

    public static final String TYPE = "batch";

    public CQLWeighableMarshallableBatch() {
    }

    public CQLWeighableMarshallableBatch(String batchType, List<String> queries, List<List<ByteBuffer>> values, QueryOptions queryOptions, long batchTimeMillis)
    {
        super(queryOptions, batchTimeMillis);
        this.queries = queries;
        this.values = values;
        this.batchType = batchType;
        boolean success = false;
        try
        {

            //weight, batch type, queries, values
            int weightTemp = 8 + FullQueryLogger.EMPTY_LIST_SIZE + FullQueryLogger.EMPTY_LIST_SIZE;
            for (int ii = 0; ii < queries.size(); ii++)
            {
                weightTemp += ObjectSizes.sizeOf(queries.get(ii));
            }

            weightTemp += FullQueryLogger.EMPTY_LIST_SIZE * values.size();
            for (int ii = 0; ii < values.size(); ii++)
            {
                List<ByteBuffer> sublist = values.get(ii);
                weightTemp += FullQueryLogger.EMPTY_BYTEBUFFER_SIZE * sublist.size();
                for (int zz = 0; zz < sublist.size(); zz++)
                {
                    weightTemp += sublist.get(zz).capacity();
                }
            }
            weightTemp += super.weight();
            weightTemp += ObjectSizes.sizeOf(batchType);
            weight = weightTemp;
            success = true;
        }
        finally
        {
            if (!success)
            {
                release();
            }
        }
    }

    String type()
    {
        return TYPE;
    }

    @Override
    public void writeMarshallable(WireOut wire)
    {
        super.writeMarshallable(wire);
        wire.write("batch-type").text(batchType);
        ValueOut valueOut = wire.write("queries");
        valueOut.int32(queries.size());
        for (String query : queries)
        {
            valueOut.text(query);
        }
        valueOut = wire.write("values");
        valueOut.int32(values.size());
        for (List<ByteBuffer> subValues : values)
        {
            valueOut.int32(subValues.size());
            for (ByteBuffer value : subValues)
            {
                valueOut.bytes(BytesStore.wrap(value));
            }
        }
    }

    @Override
    public void readMarshallable(WireIn wire) {
        super.readMarshallable(wire);
        batchType = wire.read(batchType).text();
        ValueIn valueIn = wire.read("queries");
        int querySize = valueIn.int32();
        List<String> queries = new ArrayList<>(querySize);
        for(int idx = 0; idx < querySize; idx++) {
            queries.add(valueIn.text());
        }

        valueIn = wire.read("values");
        int numValues = valueIn.int32();
        List<List<ByteBuffer>> values = new ArrayList<>();
        for (int idx = 0; idx < numValues; idx++)
        {
            List<ByteBuffer> subValues = new ArrayList<>();
            values.add(subValues);
            int numSubValues = valueIn.int32();
            for (int subIdx = 0; subIdx < numSubValues; subIdx++)
            {
                subValues.add(ByteBuffer.wrap(valueIn.bytes()));
            }
        }
    }

    @Override
    public int weight()
    {
        return weight;
    }
}
