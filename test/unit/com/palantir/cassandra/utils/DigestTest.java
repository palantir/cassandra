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

package com.palantir.cassandra.utils;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.ByteArrayAssert;

public class DigestTest
{
    @Test
    public void testDigestIsNotCommunative() throws Exception
    {
        MessageDigest digest1 = MessageDigest.getInstance("MD5");
        MessageDigest digest2 = MessageDigest.getInstance("MD5");


        byte[] a = new byte[] {'a'};
        byte[] b = new byte[] {'b'};

        digest1.update(a);
        digest1.update(b);

        digest2.update(b);
        digest2.update(a);

        new ByteArrayAssert(digest1.digest()).isNotEqualTo(digest2.digest());
    }

    @Test
    public void testLongUpdateEquivalence() throws Exception
    {
        MessageDigest digest1 = MessageDigest.getInstance("MD5");
        MessageDigest digest2 = MessageDigest.getInstance("MD5");

        ByteBuffer longBuffer = ByteBuffer.allocate(8);
        longBuffer.putLong(0, 42);
        digest1.update(longBuffer.array(), 0, 8);

        FBUtilities.updateWithLong(digest2, 42);

        new ByteArrayAssert(digest1.digest()).isEqualTo(digest2.digest());
    }
}
