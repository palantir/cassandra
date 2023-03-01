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
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.Util.token;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

@RunWith(OrderedJUnit4ClassRunner.class)
public class TokenMetadataTest
{
    public final static String ONE = "1";
    public final static String SIX = "6";

    private final ExecutorService executorService = ForkJoinPool.commonPool();

    static TokenMetadata tmd;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(token(ONE), InetAddress.getByName("127.0.0.1"));
        tmd.updateNormalToken(token(SIX), InetAddress.getByName("127.0.0.6"));
    }

    private static void testRingIterator(ArrayList<Token> ring, String start, boolean includeMin, String... expected)
    {
        ArrayList<Token> actual = new ArrayList<>();
        Iterators.addAll(actual, TokenMetadata.ringIterator(ring, token(start), includeMin));
        assertEquals(actual.toString(), expected.length, actual.size());
        for (int i = 0; i < expected.length; i++)
            assertEquals("Mismatch at index " + i + ": " + actual, token(expected[i]), actual.get(i));
    }

    private void testMethodLock(Semaphore semaphore, Runnable runnable) {
        executorService.execute(() -> {
            try
            {
                runnable.run();
                semaphore.release();
            }
            catch (Exception e)
            {
            }
        });
    }

    @Test
    public void testPublicLock() throws InterruptedException, UnknownHostException
    {
        InetAddress inetAddress = InetAddress.getByName("127.0.0.1");
        InetAddress replaceInetAddress = InetAddress.getByName("127.0.0.10");
        Semaphore semaphore = new Semaphore(0);
        tmd.lock();
        testMethodLock(semaphore, () -> tmd.pendingRangeChanges(inetAddress));
        testMethodLock(semaphore, () -> tmd.isMember(inetAddress));
        testMethodLock(semaphore, () -> tmd.isLeaving(inetAddress));
        testMethodLock(semaphore, () -> tmd.isMoving(inetAddress));
        testMethodLock(semaphore, () -> tmd.cloneOnlyTokenMap());
        testMethodLock(semaphore, () -> tmd.cachedOnlyTokenMap());
        testMethodLock(semaphore, () -> tmd.cloneAfterAllLeft());
        testMethodLock(semaphore, () -> tmd.cloneAfterAllSettled());
        testMethodLock(semaphore, () -> tmd.addLeavingEndpoint(inetAddress));
        testMethodLock(semaphore, () -> tmd.addMovingEndpoint(token(ONE), inetAddress));
        testMethodLock(semaphore, () -> tmd.addBootstrapTokens(Arrays.asList(token(ONE)), inetAddress));
        testMethodLock(semaphore, () -> tmd.addReplaceTokens(Arrays.asList(token(ONE)), inetAddress, replaceInetAddress));
        testMethodLock(semaphore, () -> tmd.removeEndpoint(inetAddress));
        testMethodLock(semaphore, () -> tmd.removeFromMoving(inetAddress));
        testMethodLock(semaphore, () -> tmd.removeBootstrapTokens(Arrays.asList(token(ONE))));
        testMethodLock(semaphore, () -> tmd.updateNormalToken(token(ONE), inetAddress));
        testMethodLock(semaphore, () -> tmd.updateHostId(UUID.randomUUID(), inetAddress));
        testMethodLock(semaphore, () -> tmd.getEndpointForHostId(UUID.randomUUID()));
        testMethodLock(semaphore, () -> tmd.getEndpointToHostIdMapForReading());
        testMethodLock(semaphore, () -> tmd.getHostId(inetAddress));
        testMethodLock(semaphore, () -> tmd.getTokens(inetAddress));
        testMethodLock(semaphore, () -> tmd.getBootstrapTokens());
        testMethodLock(semaphore, () -> tmd.getAllEndpoints());
        testMethodLock(semaphore, () -> tmd.getLeavingEndpoints());
        testMethodLock(semaphore, () -> tmd.getMovingEndpoints());
        testMethodLock(semaphore, () -> tmd.getEndpoint(token(ONE)));
        testMethodLock(semaphore, () -> tmd.updateTopology(inetAddress));
        testMethodLock(semaphore, () -> tmd.updateTopology());
        testMethodLock(semaphore, () -> tmd.clearUnsafe());
        testMethodLock(semaphore, () -> tmd.getEndpointToTokenMapForReading());
        testMethodLock(semaphore, () -> tmd.getNormalAndBootstrappingTokenToEndpointMap());
        assertFalse(semaphore.tryAcquire(1, TimeUnit.SECONDS));
        tmd.unlock();
        assertTrue(semaphore.tryAcquire(10, TimeUnit.SECONDS));
    }

    @Test
    public void testRingIterator()
    {
        ArrayList<Token> ring = tmd.sortedTokens();
        testRingIterator(ring, "2", false, "6", "1");
        testRingIterator(ring, "7", false, "1", "6");
        testRingIterator(ring, "0", false, "1", "6");
        testRingIterator(ring, "", false, "1", "6");
    }

    @Test
    public void testRingIteratorIncludeMin()
    {
        ArrayList<Token> ring = tmd.sortedTokens();
        testRingIterator(ring, "2", true, "6", "", "1");
        testRingIterator(ring, "7", true, "", "1", "6");
        testRingIterator(ring, "0", true, "1", "6", "");
        testRingIterator(ring, "", true, "1", "6", "");
    }

    @Test
    public void testRingIteratorEmptyRing()
    {
        testRingIterator(new ArrayList<Token>(), "2", false);
    }

    @Test
    public void testTopologyUpdate_RackConsolidation() throws UnknownHostException
    {
        final InetAddress first = InetAddress.getByName("127.0.0.1");
        final InetAddress second = InetAddress.getByName("127.0.0.6");
        final String DATA_CENTER = "datacenter1";
        final String RACK1 = "rack1";
        final String RACK2 = "rack2";

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddress endpoint)
            {
                return endpoint.equals(first) ? RACK1 : RACK2;
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }
        });

        tmd.updateNormalToken(token(ONE), first);
        tmd.updateNormalToken(token(SIX), second);

        TokenMetadata tokenMetadata = tmd.cloneOnlyTokenMap();
        assertNotNull(tokenMetadata);

        TokenMetadata.Topology topology = tokenMetadata.getTopology();
        assertNotNull(topology);

        Multimap<String, InetAddress> allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        Map<String, Multimap<String, InetAddress>> racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK2).contains(second));

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddress endpoint)
            {
                return RACK1;
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }
        });

        tokenMetadata.updateTopology(first);
        tokenMetadata.updateTopology(second);

        allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertFalse(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(second));
    }

    @Test
    public void testTopologyUpdate_RackExpansion() throws UnknownHostException
    {
        final InetAddress first = InetAddress.getByName("127.0.0.1");
        final InetAddress second = InetAddress.getByName("127.0.0.6");
        final String DATA_CENTER = "datacenter1";
        final String RACK1 = "rack1";
        final String RACK2 = "rack2";

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddress endpoint)
            {
                return RACK1;
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }
        });

        tmd.updateNormalToken(token(ONE), first);
        tmd.updateNormalToken(token(SIX), second);

        TokenMetadata tokenMetadata = tmd.cloneOnlyTokenMap();
        assertNotNull(tokenMetadata);

        TokenMetadata.Topology topology = tokenMetadata.getTopology();
        assertNotNull(topology);

        Multimap<String, InetAddress> allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        Map<String, Multimap<String, InetAddress>> racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertFalse(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(second));

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddress endpoint)
            {
                return endpoint.equals(first) ? RACK1 : RACK2;
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }
        });

        tokenMetadata.updateTopology();

        allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK2).contains(second));
    }
}
