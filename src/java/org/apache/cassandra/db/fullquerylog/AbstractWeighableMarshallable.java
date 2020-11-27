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

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.utils.binlog.BinLog;
import org.apache.cassandra.utils.concurrent.WeightedQueue;

public abstract class AbstractWeighableMarshallable extends BinLog.ReleaseableWriteMarshallable implements WeightedQueue.Weighable, ReadMarshallable
{
    public static final String METHOD_FIELD = "method";

    public enum Method {
        THRIFT, CQL;
    }

    abstract Method method();

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(METHOD_FIELD).text(method().name());
    }

    public static Method readMethod(WireIn wire) {
        return Method.valueOf(wire.read(METHOD_FIELD).text());
    }
}
