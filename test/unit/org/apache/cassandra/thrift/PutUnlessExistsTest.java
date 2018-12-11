/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.thrift;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;


public class PutUnlessExistsTest
{
    private static final String KEYSPACE1 = "PutUnlessExistsTest";
    private static final String CF_STANDARD = "Standard1";

    private static final String COLUMN_1 = "col1";
    private static final String COLUMN_2 = "col2";
    private static final String VALUE_1 = "val1";
    private static final String VALUE_2 = "val2";
    
    private static CassandraServer server;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException, IOException, TException
    {
        SchemaLoader.prepareServer();
        new EmbeddedCassandraService().start();
        ThriftSessionManager.instance.setCurrentSocket(new InetSocketAddress(9160));
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));
        server = new CassandraServer();
        server.set_keyspace(KEYSPACE1);
    }

    private CASResult makePutUnlessExistsRequest(String key, String columnName, String value)
    {
        return makePutUnlessExistsRequest(key, ImmutableMap.of(columnName, value));
    }

    private CASResult makePutUnlessExistsRequest(String key, Map<String, String> columns) {
        ByteBuffer bbKey = ByteBufferUtil.bytes(key);
        List<Column> cols = new ArrayList<Column>();
        for (Map.Entry<String, String> column : columns.entrySet()) {
            cols.add(new Column(ByteBufferUtil.bytes(column.getKey()))
                    .setValue(ByteBufferUtil.bytes(column.getValue()))
                    .setTimestamp(System.currentTimeMillis()));
        }

        try {
            return server.put_unless_exists(bbKey, CF_STANDARD, cols, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.ONE);
        } catch (InvalidRequestException | UnavailableException | TimedOutException e) {
            throw new RuntimeException(e);
        }
    }
    
    private ColumnOrSuperColumn fetchColumnValues(String key, String columnName) throws NotFoundException {
        ByteBuffer bbKey = ByteBufferUtil.bytes(key);
        ColumnPath cp = new ColumnPath(CF_STANDARD);
        cp.setColumn(ByteBufferUtil.bytes(columnName));
        
        try {
            return server.get(bbKey, cp, ConsistencyLevel.LOCAL_SERIAL);
        } catch (InvalidRequestException | UnavailableException | TimedOutException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Test
    public void testPutUnlessExistsSemanticsOnSingleColumn() throws NotFoundException
    {
        String key = "single_column_test";

        // first pue attempt is succesfull
        CASResult result = makePutUnlessExistsRequest(key, COLUMN_1, VALUE_1);
        Assert.assertTrue(result.isSetSuccess());
        Assert.assertTrue(result.isSuccess());

        ColumnOrSuperColumn value = fetchColumnValues(key, COLUMN_1);
        Assert.assertTrue(value.isSetColumn());
        Assert.assertEquals(new String(value.getColumn().getValue()), VALUE_1);

        // second attempt fails and value is still first pue value
        result = makePutUnlessExistsRequest(key, COLUMN_1, VALUE_2);
        Assert.assertTrue(result.isSetSuccess());
        Assert.assertFalse(result.isSuccess());
        
        value = fetchColumnValues(key, COLUMN_1);
        Assert.assertTrue(value.isSetColumn());
        Assert.assertEquals(new String(value.getColumn().getValue()), VALUE_1);
    }

    @Test
    public void testPutUnlessExistsSucceedsForUniqueColumn() throws NotFoundException {
        String key = "same_key_different_columns";

        // set up first column
        CASResult result = makePutUnlessExistsRequest(key, COLUMN_1, VALUE_1);
        Assert.assertTrue(result.isSetSuccess());
        Assert.assertTrue(result.isSuccess());

        ColumnOrSuperColumn value = fetchColumnValues(key, COLUMN_1);
        Assert.assertTrue(value.isSetColumn());
        Assert.assertEquals(new String(value.getColumn().getValue()), VALUE_1);

        // a PUE of a different column in same column family should succeed
        result = makePutUnlessExistsRequest(key, COLUMN_2, VALUE_2);
        Assert.assertTrue(result.isSetSuccess());
        Assert.assertTrue(result.isSuccess());

        value = fetchColumnValues(key, COLUMN_2);
        Assert.assertTrue(value.isSetColumn());
        Assert.assertEquals(new String(value.getColumn().getValue()), VALUE_2);
    }

    @Test(expected = NotFoundException.class)
    public void testPutUnlessExistsFailsIfAnyColumnOverlaps() throws NotFoundException {
        String key = "same_key_overlapping_columns";

        // set up first column
        CASResult result = makePutUnlessExistsRequest(key, COLUMN_1, VALUE_1);
        Assert.assertTrue(result.isSetSuccess());
        Assert.assertTrue(result.isSuccess());

        ColumnOrSuperColumn value = fetchColumnValues(key, COLUMN_1);
        Assert.assertTrue(value.isSetColumn());
        Assert.assertEquals(new String(value.getColumn().getValue()), VALUE_1);

        // a PUE with any overlap with existing columns should fail
        result = makePutUnlessExistsRequest(key, ImmutableMap.of(COLUMN_1, "this-write-fails", COLUMN_2, VALUE_2));
        Assert.assertTrue(result.isSetSuccess());
        Assert.assertFalse(result.isSuccess());

        value = fetchColumnValues(key, COLUMN_1);
        Assert.assertTrue(value.isSetColumn());
        Assert.assertEquals(new String(value.getColumn().getValue()), VALUE_1);

        fetchColumnValues(key, COLUMN_2);
    }
}
