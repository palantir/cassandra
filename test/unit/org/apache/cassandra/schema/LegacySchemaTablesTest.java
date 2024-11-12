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

package org.apache.cassandra.schema;

import java.util.UUID;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.locator.SimpleStrategy;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(OrderedJUnit4ClassRunner.class)
public class LegacySchemaTablesTest
{
    private static final String KEYSPACE1 = "Keyspace1";
    private static final String KEYSPACE2 = "Keyspace2";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_STANDARD2 = "Standard2";

    @Test
    public void testSchemaToMutationsCache() {
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2));
        UUID initialSchemaUUID = Schema.instance.getVersion();
        LegacySchemaTables.convertSchemaToMutations();
        Assert.assertEquals(LegacySchemaTables.mutations.size(), 1);
        Assert.assertTrue(LegacySchemaTables.mutations.asMap().containsKey(initialSchemaUUID));

        SchemaLoader.createKeyspace(KEYSPACE2,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2));
        UUID updatedSchemaUUID = Schema.instance.getVersion();
        LegacySchemaTables.convertSchemaToMutations();
        Assert.assertEquals(LegacySchemaTables.mutations.size(), 1);
        Assert.assertFalse(LegacySchemaTables.mutations.asMap().containsKey(initialSchemaUUID));
        Assert.assertTrue(LegacySchemaTables.mutations.asMap().containsKey(updatedSchemaUUID));
    }
}
