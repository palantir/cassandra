/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.dht;

import java.math.BigInteger;
import java.util.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.config.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.Util.dk;


/**
 * Test cases where multiple keys collides, ie have the same token.
 * Order preserving partitioner have no possible collision and creating
 * collision for the RandomPartitioner is ... difficult, so we create a dumb
 * length partitioner that takes the length of the key as token, making
 * collision easy and predictable.
 */
public class KeyCollisionTest
{
    static IPartitioner oldPartitioner;
    private static final String KEYSPACE1 = "KeyCollisionTest1";
    private static final String CF = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        oldPartitioner = DatabaseDescriptor.getPartitioner();
        DatabaseDescriptor.setPartitioner(LengthPartitioner.instance);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF));
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setPartitioner(oldPartitioner);
    }

    @Test
    public void testGetSliceWithCollision() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.clearUnsafe();

        insert("k1", "k2", "kq");       // token = 2, kq ordered after row below lexicographically
        insert("key1", "key2", "key3"); // token = 4
        insert("longKey1", "longKey2"); // token = 8

        List<Row> rows = cfs.getRangeSlice(new Bounds<RowPosition>(dk("k2"), dk("key2")), null, new IdentityQueryFilter(), 10000);
        assert rows.size() == 4 : "Expecting 4 keys, got " + rows.size();
        assert rows.get(0).key.getKey().equals(ByteBufferUtil.bytes("k2"));
        assert rows.get(1).key.getKey().equals(ByteBufferUtil.bytes("kq"));
        assert rows.get(2).key.getKey().equals(ByteBufferUtil.bytes("key1"));
        assert rows.get(3).key.getKey().equals(ByteBufferUtil.bytes("key2"));
    }

    private void insert(String... keys)
    {
        for (String key : keys)
            insert(key);
    }

    private void insert(String key)
    {
        Mutation rm;
        rm = new Mutation(KEYSPACE1, ByteBufferUtil.bytes(key));
        rm.add(CF, Util.cellname("column"), ByteBufferUtil.bytes("asdf"), 0);
        rm.applyUnsafe();
    }

    static class BigIntegerToken extends ComparableObjectToken<BigInteger>
    {
        private static final long serialVersionUID = 1L;

        public BigIntegerToken(BigInteger token)
        {
            super(token);
        }

        // convenience method for testing
        public BigIntegerToken(String token) {
            this(new BigInteger(token));
        }

        @Override
        public IPartitioner getPartitioner()
        {
            return LengthPartitioner.instance;
        }

        @Override
        public long getHeapSize()
        {
            return 0;
        }
    }
}
