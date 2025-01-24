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

package com.palantir.cassandra.db;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.net.InetAddresses;
import org.apache.commons.lang3.StringUtils;

public final class TokenOwnershipSnapshot
{

    public static byte[] serialize(Iterable<InetAddress> owners)
    {
        return StringUtils.join(owners, ',').getBytes(StandardCharsets.UTF_8);
    }

    public static Set<InetAddress> deserialize(@Nullable byte[] serialized)
    {
        if (serialized == null)
        {
            return null;
        }
        Set<InetAddress> owners = new HashSet<>();
        for (String inetAddress : StringUtils.split(new String(serialized, StandardCharsets.UTF_8), ','))
        {
            owners.add(InetAddresses.forString(inetAddress));
        }
        return owners;
    }

    private TokenOwnershipSnapshot() {}
}
