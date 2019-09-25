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
package org.apache.cassandra.thrift;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

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

    private static EmbeddedCassandraService cassandra;
    private Cassandra.Client client = null;

    public static void setup() throws IOException
    {
        cassandra = new EmbeddedCassandraService();
        cassandra.start();
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException, IOException
    {
        SchemaLoader.prepareServer();
        setup();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    CFMetaData.Builder.create(KEYSPACE1, CF_STANDARD, true, false, false)
                                                      .addPartitionKey("pk", AsciiType.instance)
                                                      .addClusteringColumn("ck", AsciiType.instance)
                                                      .addRegularColumn("val", AsciiType.instance)
                                                      .build());
    }

    private CASResult makePutUnlessExistsRequest(String key, String columnName, String value) throws TException
    {
        return makePutUnlessExistsRequest(key, ImmutableMap.of(columnName, value));
    }

    private CASResult makePutUnlessExistsRequest(String key, Map<String, String> columns) throws TException {
        Cassandra.Client client = getClient();
        client.set_keyspace(KEYSPACE1);

        ByteBuffer key_user_id = ByteBufferUtil.bytes(key);

        long timestamp = System.currentTimeMillis();

        List<Column> cols = new ArrayList<Column>(columns.entrySet().size());
        for (Map.Entry<String, String> column : columns.entrySet()) {
            cols.add(new Column(ByteBufferUtil.bytes(column.getKey()))
                     .setValue(ByteBufferUtil.bytes(column.getValue()))
                     .setTimestamp(timestamp));
        }

        try {
           return getClient().put_unless_exists(key_user_id, CF_STANDARD, cols, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.ONE);
        } catch (InvalidRequestException | UnavailableException | TimedOutException e) {
            throw new RuntimeException(e);
        }
    }

    private ColumnOrSuperColumn fetchColumnValues(String key, String columnName) throws TException {
        ByteBuffer bbKey = ByteBufferUtil.bytes(key);
        ColumnPath cp = new ColumnPath(CF_STANDARD);
        cp.setColumn(ByteBufferUtil.bytes(columnName));

        try {
            return getClient().get(bbKey, cp, ConsistencyLevel.LOCAL_SERIAL);
        } catch (InvalidRequestException | UnavailableException | TimedOutException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testPutUnlessExistsSemanticsOnSingleColumn() throws TException
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
    public void testPutUnlessExistsSucceedsForUniqueColumn() throws TException {
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
    public void testPutUnlessExistsFailsIfAnyColumnOverlaps() throws TException {
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

    /**
     * Gets a connection to the localhost client
     *
     * @return
     * @throws TTransportException
     */
    private Cassandra.Client getClient() throws TException
    {
        TTransport tr = new TFramedTransport(new TSocket("localhost", DatabaseDescriptor.getRpcPort()));
        TProtocol proto = new TBinaryProtocol(tr);
        if (client == null)
        {
            client = new Cassandra.Client(proto);
            tr.open();
            client.set_keyspace(KEYSPACE1);
        }
        return client;
    }
}