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

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidMutationException;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MutationVerificationUtilsTest
{
    public static final String KEYSPACE5 = "Keyspace5";
    public static final String CF_STANDARD1 = "Standard1";

    public String cfname;
    public ColumnFamilyStore store;
    public InetAddress LOCAL, REMOTE;

    private boolean initialized;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        System.setProperty("palantir_cassandra.verify_keys_on_write", "true");

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE5,
                NetworkTopologyStrategy.class,
                ImmutableMap.of("datacenter1", "1"),
                SchemaLoader.standardCFMD(KEYSPACE5, CF_STANDARD1));
    }

    @Before
    public void prepare() throws Exception
    {
        if (!initialized)
        {
            SchemaLoader.startGossiper();
            initialized = true;

            LOCAL = FBUtilities.getBroadcastAddress();
            REMOTE = InetAddress.getByName("127.0.0.2");
        }

        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        StorageService.instance.setTokens(Collections.singletonList(StorageService.getPartitioner().getToken(ByteBufferUtil.bytes("a"))));
        tmd.updateNormalToken(StorageService.getPartitioner().getToken(ByteBufferUtil.bytes("b")), REMOTE);
        MutationVerificationUtils.clearLastTokenRingCacheUpdate();
    }

    @Test
    public void testValidMutation()
    {
        ByteBuffer key = ByteBufferUtil.bytes("a");
        Mutation mutation = new Mutation(KEYSPACE5, key);
        MutationVerificationUtils.verifyMutation(mutation);
    }

    @Test
    public void testInvalidMutation()
    {
        ByteBuffer key = ByteBufferUtil.bytes("b");
        Mutation mutation = new Mutation(KEYSPACE5, key);
        assertThatThrownBy(() -> MutationVerificationUtils.verifyMutation(mutation)).isInstanceOf(InvalidMutationException.class);
    }

    @Test
    public void testValidMutationDoesNotRefreshCache()
    {
        ByteBuffer key = ByteBufferUtil.bytes("a");
        Mutation mutation = new Mutation(KEYSPACE5, key);

        long initialRingVersion = StorageService.instance.getTokenMetadata().getRingVersion();
        MutationVerificationUtils.verifyMutation(mutation);
        long finalRingVersion = StorageService.instance.getTokenMetadata().getRingVersion();
        assertThat(initialRingVersion).isEqualTo(finalRingVersion);
    }

    @Test
    public void testInvalidMutationDoesRefreshCache()
    {
        ByteBuffer key = ByteBufferUtil.bytes("b");
        Mutation mutation = new Mutation(KEYSPACE5, key);

        long initialRingVersion = StorageService.instance.getTokenMetadata().getRingVersion();
        assertThatThrownBy(() -> MutationVerificationUtils.verifyMutation(mutation)).isInstanceOf(InvalidMutationException.class);
        long finalRingVersion = StorageService.instance.getTokenMetadata().getRingVersion();
        assertThat(initialRingVersion).isNotEqualTo(finalRingVersion);
    }
}
