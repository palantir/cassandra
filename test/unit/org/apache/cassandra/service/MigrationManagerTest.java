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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.net.MessagingService;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MigrationManagerTest
{
    private static final UUID UUID_1 = UUID.randomUUID();
    private static final UUID UUID_2 = UUID.randomUUID();

    private static InetAddress HOST_1;
    private static InetAddress HOST_2;
    private static InetAddress HOST_3;
    private static InetAddress HOST_4;

    @BeforeClass
    public static void setup() throws UnknownHostException
    {
        HOST_1 = InetAddress.getByName("10.0.0.1");
        HOST_2 = InetAddress.getByName("10.0.0.2");
        HOST_3 = InetAddress.getByName("10.0.0.3");
        HOST_4 = InetAddress.getByName("10.0.0.4");
        MessagingService.instance().setVersion(HOST_1, MessagingService.VERSION_22);
        MessagingService.instance().setVersion(HOST_2, MessagingService.VERSION_22);
        MessagingService.instance().setVersion(HOST_3, MessagingService.VERSION_22);
        MessagingService.instance().setVersion(HOST_4, MessagingService.VERSION_22);
    }

    @Test
    public void shouldPullSchemaIfNoOutstandingRequests()
    {
        assertTrue(MigrationManager.shouldPullSchemaFrom(HOST_1, UUID_1));
    }

    @Test
    public void onlyRequestOncePerEndpointVersion() {
        MigrationManager.addEndpointToSchemaPullVersion(UUID_1, HOST_1);
        assertFalse(MigrationManager.shouldPullSchemaFrom(HOST_1, UUID_1));
    }

    @Test
    public void removalAllowsForPull() {
        MigrationManager.addEndpointToSchemaPullVersion(UUID_1, HOST_1);
        MigrationManager.removeEndpointFromSchemaPullVersion(UUID_1, HOST_1);
        assertTrue(MigrationManager.shouldPullSchemaFrom(HOST_1, UUID_1));
    }

    @Test
    public void multipleSchemaVersionsDontInteract() {
        MigrationManager.addEndpointToSchemaPullVersion(UUID_1, HOST_1);
        assertTrue(MigrationManager.shouldPullSchemaFrom(HOST_1, UUID_2));
    }

    @Test
    public void noRequestOverMaxOutstanding() {
        MigrationManager.addEndpointToSchemaPullVersion(UUID_1, HOST_1);
        MigrationManager.addEndpointToSchemaPullVersion(UUID_1, HOST_2);
        MigrationManager.addEndpointToSchemaPullVersion(UUID_1, HOST_3);
        assertFalse(MigrationManager.shouldPullSchemaFrom(HOST_4, UUID_1));
    }
}
