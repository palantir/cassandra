/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.palantir.cassandra.cvim.CrossVpcIpMappingHandshaker;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WindowsFailedSnapshotTracker;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.ByteOrderedPartitioner;import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.OrderPreservingPartitioner.StringToken;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.PropertyFileSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;

@RunWith(OrderedJUnit4ClassRunner.class)
public class StorageServiceServerTest
{
    @BeforeClass
    public static void setUp() throws ConfigurationException
    {
        DatabaseDescriptor.setDaemonInitialized();
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);
        Keyspace.setInitialized();
    }

    @Before
    public void setStartMode() {
        StorageService.instance.setOperationMode(StorageService.Mode.STARTING);
    }

    @After
    public void after() {
        DatabaseDescriptor.setCrossVpcInternodeCommunication(false);
        DatabaseDescriptor.setCrossVpcHostnameSwapping(false);
        DatabaseDescriptor.setCrossVpcIpSwapping(false);
    }

    @Test
    public void isCommitlogEmptyForBootstrap_returnsTrueWhenEmpty() {
        assertThat(StorageService.isCommitlogEmptyForBootstrap(ImmutableSet.of())).isTrue();
    }

    @Test
    public void isCommitlogEmptyForBootstrap_returnsFalseWhenNotEmpty() {
        assertThat(StorageService.isCommitlogEmptyForBootstrap(ImmutableSet.of(UUID.randomUUID()))).isFalse();
    }

    @Test
    public void isCommitlogEmptyForBootstrap_returnsTrueIgnoresSystemCfs() {
        Set<UUID> uuids = Schema.instance.getKeyspaceMetaData(SystemKeyspace.NAME).values().stream().map(cf -> cf.cfId).collect(Collectors.toSet());
        assertThat(StorageService.isCommitlogEmptyForBootstrap(uuids)).isTrue();
    }

    @Test
    public void testRegularMode() throws ConfigurationException
    {
        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        StorageService.instance.initServer(0);
        for (String path : DatabaseDescriptor.getAllDataFileLocations())
        {
            // verify that storage directories are there.
            assertTrue(new File(path).exists());
        }
        // a proper test would be to call decommission here, but decommission() mixes both shutdown and datatransfer
        // calls.  This test is only interested in the shutdown-related items which a properly handled by just
        // stopping the client.
        //StorageService.instance.decommission();
        StorageService.instance.stopClient();
    }

    @Test
    public void disableNode_canOnlyDisableWhenNormal() {
        for(StorageService.Mode mode : StorageService.Mode.values()) {
            StorageService.instance.setOperationMode(mode);
            if(mode.equals(StorageService.Mode.NORMAL)) {
                mode = StorageService.Mode.DISABLED;
            }
            StorageService.instance.disableNode();
            assertThat(StorageService.instance.getOperationMode()).isEqualTo(mode.name());
        }
    }

    @Test
    public void disableNode_stopsCrossVpcHandshake() {
        StorageService.instance.initServer(0);
        CassandraDaemon daemon = mock(CassandraDaemon.class);
        CassandraDaemon.Server server = mock(CassandraDaemon.Server.class);
        daemon.thriftServer = server;
        daemon.nativeServer = server;
        StorageService.instance.registerDaemon(daemon);
        DatabaseDescriptor.setCrossVpcInternodeCommunication(true);
        CrossVpcIpMappingHandshaker.instance.start();
        StorageService.instance.unsafeEnableNode();
        assertThat(CrossVpcIpMappingHandshaker.instance.isEnabled()).isTrue();
        StorageService.instance.disableNode();
        assertThat(CrossVpcIpMappingHandshaker.instance.isEnabled()).isFalse();
    }

    @Test
    public void enableNode_canOnlyEnableWhenDisabled() {
        StorageService.instance.initServer(0);
        CassandraDaemon daemon = mock(CassandraDaemon.class);
        CassandraDaemon.Server server = mock(CassandraDaemon.Server.class);
        daemon.thriftServer = server;
        daemon.nativeServer = server;
        StorageService.instance.registerDaemon(daemon);
        for(StorageService.Mode mode : StorageService.Mode.values()) {
            StorageService.instance.setOperationMode(mode);
            if(mode.equals(StorageService.Mode.DISABLED)) {
                mode = StorageService.Mode.NORMAL;
            }
            StorageService.instance.enableNode();
            assertThat(StorageService.instance.getOperationMode()).isEqualTo(mode.toString());
        }
    }

    @Test
    public void enableNode_startsCrossVpcHandshake() {
        DatabaseDescriptor.setCrossVpcInternodeCommunication(true);
        StorageService.instance.initServer(0);
        CassandraDaemon daemon = mock(CassandraDaemon.class);
        CassandraDaemon.Server server = mock(CassandraDaemon.Server.class);
        daemon.thriftServer = server;
        daemon.nativeServer = server;
        StorageService.instance.registerDaemon(daemon);
        StorageService.instance.disableNode();
        CrossVpcIpMappingHandshaker.instance.stop();
        assertThat(CrossVpcIpMappingHandshaker.instance.isEnabled()).isFalse();
        StorageService.instance.enableNode();
        assertThat(CrossVpcIpMappingHandshaker.instance.isEnabled()).isTrue();
        CrossVpcIpMappingHandshaker.instance.stop();
    }

    @Test
    public void testGetAllRangesEmpty()
    {
        List<Token> toks = Collections.emptyList();
        assertEquals(Collections.<Range<Token>>emptyList(), StorageService.instance.getAllRanges(toks));
    }

    @Test
    public void testSnapshot() throws IOException
    {
        // no need to insert extra data, even an "empty" database will have a little information in the system keyspace
        StorageService.instance.clearSnapshot("snapshot");
        StorageService.instance.takeSnapshot("snapshot");
    }

    @Test
    public void testSnapshotJoiningNode() throws IOException
    {
        StorageService ss = StorageService.instance;
        ss.setOperationMode(StorageService.Mode.JOINING);
        ss.clearSnapshot("joiningSnapshot");
        ss.takeSnapshot("joiningSnapshot");
    }

    private void checkTempFilePresence(File f, boolean exist)
    {
        for (int i = 0; i < 5; i++)
        {
            File subdir = new File(f, Integer.toString(i));
            subdir.mkdir();
            for (int j = 0; j < 5; j++)
            {
                File subF = new File(subdir, Integer.toString(j));
                assert(exist ? subF.exists() : !subF.exists());
            }
        }
    }

    @Test
    public void testSnapshotFailureHandler() throws IOException
    {
        assumeTrue(FBUtilities.isWindows());

        // Initial "run" of Cassandra, nothing in failed snapshot file
        WindowsFailedSnapshotTracker.deleteOldSnapshots();

        File f = new File(System.getenv("TEMP") + File.separator + Integer.toString(new Random().nextInt()));
        f.mkdir();
        f.deleteOnExit();
        for (int i = 0; i < 5; i++)
        {
            File subdir = new File(f, Integer.toString(i));
            subdir.mkdir();
            for (int j = 0; j < 5; j++)
                new File(subdir, Integer.toString(j)).createNewFile();
        }

        checkTempFilePresence(f, true);

        // Confirm deletion is recursive
        for (int i = 0; i < 5; i++)
            WindowsFailedSnapshotTracker.handleFailedSnapshot(new File(f, Integer.toString(i)));

        assert new File(WindowsFailedSnapshotTracker.TODELETEFILE).exists();

        // Simulate shutdown and restart of C* node, closing out the list of failed snapshots.
        WindowsFailedSnapshotTracker.resetForTests();

        // Perform new run, mimicking behavior of C* at startup
        WindowsFailedSnapshotTracker.deleteOldSnapshots();
        checkTempFilePresence(f, false);

        // Check to make sure we don't delete non-temp, non-datafile locations
        WindowsFailedSnapshotTracker.resetForTests();
        PrintWriter tempPrinter = new PrintWriter(new FileWriter(WindowsFailedSnapshotTracker.TODELETEFILE, true));
        tempPrinter.println(".safeDir");
        tempPrinter.close();

        File protectedDir = new File(".safeDir");
        protectedDir.mkdir();
        File protectedFile = new File(protectedDir, ".safeFile");
        protectedFile.createNewFile();

        WindowsFailedSnapshotTracker.handleFailedSnapshot(protectedDir);
        WindowsFailedSnapshotTracker.deleteOldSnapshots();

        assert protectedDir.exists();
        assert protectedFile.exists();

        protectedFile.delete();
        protectedDir.delete();
    }

    @Test
    public void testColumnFamilySnapshot() throws IOException
    {
        // no need to insert extra data, even an "empty" database will have a little information in the system keyspace
        StorageService.instance.takeColumnFamilySnapshot(SystemKeyspace.NAME, LegacySchemaTables.KEYSPACES, "cf_snapshot");
    }

    @Test
    public void testPrimaryRangeForEndpointWithinDCWithNetworkTopologyStrategy() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.2"));

        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddress.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "1");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name,
                                                                                                            InetAddress.getByName("127.0.0.1"));
        assertEquals(2, primaryRanges.size());
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("A"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D"))));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.2"));
        assertEquals(2, primaryRanges.size());
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C"))));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.4"));
        assertEquals(2, primaryRanges.size());
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("A"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B"))));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.5"));
        assertEquals(2, primaryRanges.size());
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D"))));
    }

    @Test
    public void testPrimaryRangesWithNetworkTopologyStrategy() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.2"));
        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddress.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "1");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.4"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.5"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D")));
    }

    @Test
    public void testPrimaryRangesWithNetworkTopologyStrategyOneDCOnly() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.2"));
        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddress.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC2", "2");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        // endpoints in DC1 should not have primary range
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.isEmpty();

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.isEmpty();

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.4"));
        assert primaryRanges.size() == 2;
        assert primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("A")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.5"));
        assert primaryRanges.size() == 2;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
    }

    @Test
    public void testPrimaryRangeForEndpointWithinDCWithNetworkTopologyStrategyOneDCOnly() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.2"));
        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddress.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC2", "2");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        // endpoints in DC1 should not have primary range
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.1"));
        assertTrue(primaryRanges.isEmpty());

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name,
                                                                                   InetAddress.getByName("127.0.0.2"));
        assertTrue(primaryRanges.isEmpty());

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.4"));
        assertTrue(primaryRanges.size() == 2);
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("A"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B"))));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.5"));
        assertTrue(primaryRanges.size() == 2);
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C"))));
    }

    @Test
    public void testPrimaryRangesWithVnodes() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        // DC1
        Multimap<InetAddress, Token> dc1 = HashMultimap.create();
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("A"));
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("E"));
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("H"));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("C"));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("I"));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("J"));
        metadata.updateNormalTokens(dc1);
        // DC2
        Multimap<InetAddress, Token> dc2 = HashMultimap.create();
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("B"));
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("G"));
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("L"));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("D"));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("F"));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("K"));
        metadata.updateNormalTokens(dc2);

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC2", "2");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        // endpoints in DC1 should not have primary range
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.isEmpty();

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.isEmpty();

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.4"));
        assert primaryRanges.size() == 4;
        assert primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("F"), new StringToken("G")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("K"), new StringToken("L")));
        // because /127.0.0.4 holds token "B" which is the next to token "A" from /127.0.0.1,
        // the node covers range (L, A]
        assert primaryRanges.contains(new Range<Token>(new StringToken("L"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.5"));
        assert primaryRanges.size() == 8;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("E"), new StringToken("F")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("J"), new StringToken("K")));
        // ranges from /127.0.0.1
        assert primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("E")));
        // the next token to "H" in DC2 is "K" in /127.0.0.5, so (G, H] goes to /127.0.0.5
        assert primaryRanges.contains(new Range<Token>(new StringToken("G"), new StringToken("H")));
        // ranges from /127.0.0.2
        assert primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("H"), new StringToken("I")));
        assert primaryRanges.contains(new Range<Token>(new StringToken("I"), new StringToken("J")));
    }

    @Test
    public void testPrimaryRangeForEndpointWithinDCWithVnodes() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        // DC1
        Multimap<InetAddress, Token> dc1 = HashMultimap.create();
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("A"));
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("E"));
        dc1.put(InetAddress.getByName("127.0.0.1"), new StringToken("H"));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("C"));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("I"));
        dc1.put(InetAddress.getByName("127.0.0.2"), new StringToken("J"));
        metadata.updateNormalTokens(dc1);

        // DC2
        Multimap<InetAddress, Token> dc2 = HashMultimap.create();
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("B"));
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("G"));
        dc2.put(InetAddress.getByName("127.0.0.4"), new StringToken("L"));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("D"));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("F"));
        dc2.put(InetAddress.getByName("127.0.0.5"), new StringToken("K"));
        metadata.updateNormalTokens(dc2);

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "2");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        // endpoints in DC1 should have primary ranges which also cover DC2
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.1"));
        assertEquals(8, primaryRanges.size());
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("J"), new StringToken("K"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("K"), new StringToken("L"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("L"), new StringToken("A"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("E"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("E"), new StringToken("F"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("F"), new StringToken("G"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("G"), new StringToken("H"))));

        // endpoints in DC1 should have primary ranges which also cover DC2
        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.2"));
        assertEquals(4, primaryRanges.size());
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("H"), new StringToken("I"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("I"), new StringToken("J"))));

        // endpoints in DC2 should have primary ranges which also cover DC1
        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.4"));
        assertEquals(4, primaryRanges.size());
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("F"), new StringToken("G"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("K"), new StringToken("L"))));
        // because /127.0.0.4 holds token "B" which is the next to token "A" from /127.0.0.1,
        // the node covers range (L, A]
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("L"), new StringToken("A"))));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.5"));
        assertTrue(primaryRanges.size() == 8);
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("D"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("E"), new StringToken("F"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("J"), new StringToken("K"))));
        // ranges from /127.0.0.1
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("D"), new StringToken("E"))));
        // the next token to "H" in DC2 is "K" in /127.0.0.5, so (G, H] goes to /127.0.0.5
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("G"), new StringToken("H"))));
        // ranges from /127.0.0.2
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("H"), new StringToken("I"))));
        assertTrue(primaryRanges.contains(new Range<Token>(new StringToken("I"), new StringToken("J"))));
    }

    @Test
    public void getRebuiltKeyspaces_removesKeyspacesWithUnavailableRanges() throws UnknownHostException
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        Token.TokenFactory factory = StorageService.getPartitioner().getTokenFactory();

        // DC1
        metadata.updateNormalToken(factory.fromString("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(factory.fromString("C"), InetAddress.getByName("127.0.0.2"));

        // DC2
        metadata.updateNormalToken(factory.fromString("B"), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(factory.fromString("D"), InetAddress.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "1");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        // If nodes aren't up RangeStreamer's FailureDetectorSourceFilter will remove the endpoints
        // from streaming consideration
        InetAddress h2 = InetAddress.getByName("127.0.0.2");
        InetAddress h4 = InetAddress.getByName("127.0.0.4");
        InetAddress h5 = InetAddress.getByName("127.0.0.5");
        Gossiper.instance.initializeNodeUnsafe(h2, UUID.randomUUID(), 1);
        Gossiper.instance.initializeNodeUnsafe(h4, UUID.randomUUID(), 1);
        Gossiper.instance.initializeNodeUnsafe(h5, UUID.randomUUID(), 1);
        Gossiper.instance.realMarkAlive(h2, Gossiper.instance.getEndpointStateForEndpoint(h2));
        Gossiper.instance.realMarkAlive(h4, Gossiper.instance.getEndpointStateForEndpoint(h4));
        Gossiper.instance.realMarkAlive(h5, Gossiper.instance.getEndpointStateForEndpoint(h5));

        Set<String> res1 = StorageService.instance.getKeyspacesWithAllRangesAvailable("DC2");
        assertThat(res1).isEmpty();

        SystemKeyspace.updateAvailableRanges("Keyspace1", StorageService.instance.getLocalRanges("Keyspace1"));
        Set<String> res2 = StorageService.instance.getKeyspacesWithAllRangesAvailable("DC2");
        assertThat(res2).containsOnly("Keyspace1");
    }

    @Test
    public void testPrimaryRangesWithSimpleStrategy() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.2"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.3"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("replication_factor", "2");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "SimpleStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangesForEndpoint(meta.name, InetAddress.getByName("127.0.0.3"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
    }

    /* Does not make much sense to use -local and -pr with simplestrategy, but just to prevent human errors */
    @Test
    public void testPrimaryRangeForEndpointWithinDCWithSimpleStrategy() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.2"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.3"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("replication_factor", "2");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "SimpleStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.1"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("C"), new StringToken("A")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.2"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("A"), new StringToken("B")));

        primaryRanges = StorageService.instance.getPrimaryRangeForEndpointWithinDC(meta.name, InetAddress.getByName("127.0.0.3"));
        assert primaryRanges.size() == 1;
        assert primaryRanges.contains(new Range<Token>(new StringToken("B"), new StringToken("C")));
    }

    @Test
    public void testCreateRepairRangeFrom() throws Exception
    {
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);

        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        metadata.updateNormalToken(new LongToken(1000L), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new LongToken(2000L), InetAddress.getByName("127.0.0.2"));
        metadata.updateNormalToken(new LongToken(3000L), InetAddress.getByName("127.0.0.3"));
        metadata.updateNormalToken(new LongToken(4000L), InetAddress.getByName("127.0.0.4"));

        Collection<Range<Token>> repairRangeFrom = StorageService.instance.createRepairRangeFrom("1500", "3700");
        assert repairRangeFrom.size() == 3;
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(1500L), new LongToken(2000L)));
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(2000L), new LongToken(3000L)));
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(3000L), new LongToken(3700L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("500", "700");
        assert repairRangeFrom.size() == 1;
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(500L), new LongToken(700L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("500", "1700");
        assert repairRangeFrom.size() == 2;
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(500L), new LongToken(1000L)));
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(1000L), new LongToken(1700L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("2500", "2300");
        assert repairRangeFrom.size() == 5;
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(2500L), new LongToken(3000L)));
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(3000L), new LongToken(4000L)));
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(4000L), new LongToken(1000L)));
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(1000L), new LongToken(2000L)));
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(2000L), new LongToken(2300L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("2000", "3000");
        assert repairRangeFrom.size() == 1;
        assert repairRangeFrom.contains(new Range<Token>(new LongToken(2000L), new LongToken(3000L)));

        repairRangeFrom = StorageService.instance.createRepairRangeFrom("2000", "2000");
        assert repairRangeFrom.size() == 0;
    }

    @Test
    public void testRecordTransientError()
    {
        StorageService.instance.clearTransientErrors();
        assertThat(StorageService.instance.getTransientErrors()).isEmpty();
        StorageService.instance.recordTransientError(StorageServiceMBean.TransientError.EXCEEDED_DISK_THRESHOLD, ImmutableMap.of());
        assertThat(StorageService.instance.getTransientErrors()).hasSize(1);
    }

    @Test
    public void testClearTransientError()
    {
        StorageService.instance.recordTransientError(StorageServiceMBean.TransientError.EXCEEDED_DISK_THRESHOLD, ImmutableMap.of());
        assertThat(StorageService.instance.getTransientErrors()).isNotEmpty();
        StorageService.instance.clearTransientErrors();
        assertThat(StorageService.instance.getTransientErrors()).isEmpty();
    }

    @Test
    public void testHasTransientError()
    {
        StorageService.instance.clearTransientErrors();
        StorageServiceMBean.TransientError error = StorageServiceMBean.TransientError.EXCEEDED_DISK_THRESHOLD;
        assertThat(StorageService.instance.hasTransientError(error)).isFalse();
        StorageService.instance.recordTransientError(error, ImmutableMap.of());
        assertThat(StorageService.instance.hasTransientError(error)).isTrue();
    }

    @Test
    public void testGetAllPresentTransientErrors()
    {
        StorageService.instance.clearTransientErrors();
        StorageServiceMBean.TransientError error = StorageServiceMBean.TransientError.EXCEEDED_DISK_THRESHOLD;
        assertThat(StorageService.instance.getPresentTransientErrorTypes()).isEmpty();
        StorageService.instance.recordTransientError(error, ImmutableMap.of());
        assertThat(StorageService.instance.getPresentTransientErrorTypes()).isEqualTo(ImmutableSet.of(error));
    }

    @Test
    public void testJoinRingThrowsWhenSystemKeyspaceHasTokens() {
        StorageService.instance.setOperationMode(StorageService.Mode.ZOMBIE);
        StorageService.instance.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        SystemKeyspace.updateTokens(ImmutableList.of(new ByteOrderedPartitioner.BytesToken(ByteBufferUtil.bytes(String.format("token%d", 0)))));
        StorageService.instance.setJoinedTestingOnly(false);
        assertThat(StorageService.instance.hasJoined()).isFalse();
        assertThat(SystemKeyspace.getSavedTokens()).isNotEmpty();
        assertThatThrownBy(() -> StorageService.instance.joinRing(ImmutableList.of(new LongToken(1).toString())))
        .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testJoinRingThrowsWhenNotZombieMode() {
        StorageService.instance.setJoinedTestingOnly(false);
        assertThat(StorageService.instance.hasJoined()).isFalse();
        StorageService.instance.setOperationMode(StorageService.Mode.NORMAL);
        assertThatThrownBy(() -> StorageService.instance.joinRing(ImmutableList.of(new LongToken(1).toString())))
        .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testJoinRingThrowsWhenTokensMalformatted() {
        StorageService.instance.setJoinedTestingOnly(false);
        assertThat(StorageService.instance.hasJoined()).isFalse();
        assertThatThrownBy(() -> StorageService.instance.joinRing(ImmutableList.of("0x00")))
        .isInstanceOf(IOException.class);
    }

    @Test
    public void getLocalHostId_handlesWhenNoEndpointPresent() {
        TokenMetadata tmd = new TokenMetadata();
        StorageService.instance.setTokenMetadataUnsafe(tmd);
        assertThat(StorageService.instance.getLocalHostId()).isNull();
    }
}
