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

package org.apache.cassandra.dht;

import java.util.Arrays;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteOrderedPartitionerTestCase extends AllocatablePartitionerTestCase
{
    public void initPartitioner()
    {
        partitioner = ByteOrderedPartitioner.instance;
    }

    public Token almostMax()
    {
        byte[] arr = Arrays.copyOf(ByteOrderedPartitioner.MAXIMUM.token, 16);
        arr[15] = 0x00;
        return new ByteOrderedPartitioner.BytesToken(arr);
    }

    @Test
    public void testIncrement()
    {
        ByteOrderedPartitioner.BytesToken token = new ByteOrderedPartitioner.BytesToken(new byte[]{ 0x00 });
        ByteOrderedPartitioner.BytesToken expected = new ByteOrderedPartitioner.BytesToken(new byte[]{ 0x00, 0x00 });
        assertEquals(expected, token.increaseSlightly());
    }

    @Test
    public void testIncrementFullLength()
    {
        byte[] tokenArr = new byte[16];
        byte[] expectedArr = new byte[16];
        Arrays.fill(tokenArr, (byte) 0x00);
        Arrays.fill(expectedArr, (byte) 0x00);
        expectedArr[15] = 0x01;
        ByteOrderedPartitioner.BytesToken token = new ByteOrderedPartitioner.BytesToken(tokenArr);
        ByteOrderedPartitioner.BytesToken expected = new ByteOrderedPartitioner.BytesToken(expectedArr);
        assertEquals(expected, token.increaseSlightly());
    }

    @Test
    public void testIncrementWrapAround()
    {
        ByteOrderedPartitioner.BytesToken token = ByteOrderedPartitioner.MAXIMUM;
        ByteOrderedPartitioner.BytesToken expected = ByteOrderedPartitioner.MINIMUM;
        assertEquals(expected, token.increaseSlightly());
    }

    @Test
    public void testSize()
    {
        ByteOrderedPartitioner.BytesToken start = new ByteOrderedPartitioner.BytesToken(new byte[]{ 0x00 });
        ByteOrderedPartitioner.BytesToken end = new ByteOrderedPartitioner.BytesToken(new byte[]{ 0x01 });
        double actual = start.size(end);
        assertEquals(1.0/256, actual, 0);
    }

    @Test
    public void testSize2()
    {
        ByteOrderedPartitioner.BytesToken start = new ByteOrderedPartitioner.BytesToken(new byte[]{ 0x00, 0x00 });
        ByteOrderedPartitioner.BytesToken end = new ByteOrderedPartitioner.BytesToken(new byte[]{ 0x00, 0x01 });
        double actual = start.size(end);
        assertEquals(1.0/256/256, actual, 0);
    }
}
