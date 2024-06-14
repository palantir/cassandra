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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class RangeStreamerTest
{
    private final String keyspace = Schema.instance.getNonSystemKeyspaces().get(0);

    @BeforeClass
    public static void setup() throws ConfigurationException
    {
        SchemaLoader.startGossiper();
        SchemaLoader.prepareServer();
        SchemaLoader.schemaDefinition("RangeStreamerTest");
    }

    @Test
    public void removeAvailableRanges_removesAlreadyStreamedRanges() throws UnknownHostException
    {
        IPartitioner p = StorageService.getPartitioner();
        Token.TokenFactory factory = p.getTokenFactory();
        Range<Token> availableRange = new Range<>(factory.fromString("0"), factory.fromString("100"));
        Range<Token> expectedToStream = new Range<>(factory.fromString("100"), factory.fromString("200"));
        Collection<Range<Token>> totalRange = ImmutableSet.of(
                                                    new Range<>(factory.fromString("0"), factory.fromString("100")),
                                                    expectedToStream);
        RangeStreamer streamer = mockRangeStreamer(totalRange, availableRange);
        streamer.addRanges(keyspace, totalRange);

        streamer.toFetch().entries().forEach(entry -> streamer.removeAvailableRanges(entry, false));
        Collection<Map.Entry<InetAddress, Collection<Range<Token>>>> values = streamer.toFetch().values();
        assertThat(values).hasSize(1);
        Collection<Range<Token>> toFetchRanges = values.iterator().next().getValue();
        assertThat(toFetchRanges).containsOnly(expectedToStream);
    }

    @Test
    public void areAllRangesPresent_trueWhenAllPresent() throws UnknownHostException
    {
        IPartitioner p = StorageService.getPartitioner();
        Token.TokenFactory factory = p.getTokenFactory();
        Range<Token> availableRange = new Range<>(factory.fromString("0"), factory.fromString("100"));
        RangeStreamer streamer = mockRangeStreamer(ImmutableSet.of(availableRange), availableRange);
        streamer.addRanges(keyspace, Arrays.asList(availableRange));
        assertThat(streamer.areAllRangesPresent()).isTrue();
    }

    @Test
    public void areAllRangesPresent_falseWhenAnyAbsent() throws UnknownHostException
    {
        IPartitioner p = StorageService.getPartitioner();
        Token.TokenFactory factory = p.getTokenFactory();
        Range<Token> availableRange = new Range<>(factory.fromString("0"), factory.fromString("100"));
        Collection<Range<Token>> totalRange = ImmutableSet.of(
                                                    new Range<>(factory.fromString("0"), factory.fromString("100")),
                                                    new Range<>(factory.fromString("100"), factory.fromString("200")));
        RangeStreamer streamer = mockRangeStreamer(totalRange, availableRange);
        streamer.addRanges(keyspace, totalRange);
        assertThat(streamer.areAllRangesPresent()).isFalse();
    }

    private RangeStreamer mockRangeStreamer(Collection<Range<Token>> totalRange, Range<Token> availableRange) throws UnknownHostException
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        Set<Token> total = new HashSet<>();
        totalRange.forEach(range -> {
            total.add(range.left);
            total.add(range.right);
        });
        tmd.updateNormalTokens(total, InetAddress.getByName("127.0.0.2"));

        StreamStateStore streamStateStore = mock(StreamStateStore.class);
        doReturn(ImmutableSet.of(availableRange)).when(streamStateStore).getAvailableRanges(anyString(), any());
        return new RangeStreamer(tmd,
                                 null,
                                 FBUtilities.getBroadcastAddress(),
                                 "Rebuild",
                                 false,
                                 DatabaseDescriptor.getEndpointSnitch(),
                                 streamStateStore,
                                 false,
                                 DatabaseDescriptor.getStreamingConnectionsPerHost());
    }
}
