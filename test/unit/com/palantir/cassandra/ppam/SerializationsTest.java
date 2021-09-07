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

package com.palantir.cassandra.ppam;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipDigest;
import org.apache.cassandra.gms.GossipDigestAck;
import org.apache.cassandra.gms.GossipDigestAck2;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.service.StorageService;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class SerializationsTest extends AbstractSerializationsTester
{
    private static final String BIN = "ppam.PrivatePublicAddressMapping.bin";

    @Test
    public void testPrivatePublicAddressMapping_syn() throws IOException
    {
        InetAddressHostname sourceName = new InetAddressHostname("localhost");
        InetAddressIp sourceInternalIp = new InetAddressIp("1.0.0.0");
        InetAddressHostname targetName = new InetAddressHostname("target");
        InetAddressIp targetExternalIp = new InetAddressIp("2.0.0.0");
        PrivatePublicAddressMappingSyn syn = new PrivatePublicAddressMappingSyn(sourceName, sourceInternalIp, targetName, targetExternalIp);

        DataOutputStreamPlus out = getOutput(BIN);
        PrivatePublicAddressMappingSyn.serializer.serialize(syn, out, getVersion());
        out.close();

        DataInputStream in = getInput(BIN);
        PrivatePublicAddressMappingSyn deserialized = PrivatePublicAddressMappingSyn.serializer.deserialize(in, getVersion());
        in.close();

        assertThat(syn).isEqualTo(deserialized);

        testSerializedSize(syn, PrivatePublicAddressMappingSyn.serializer);
    }

    @Test
    public void testPrivatePublicAddressMapping_ack() throws IOException
    {
        InetAddressHostname targetName = new InetAddressHostname("target");
        InetAddressIp targetExternalIp = new InetAddressIp("2.0.0.0");
        InetAddressIp targetInternalIp = new InetAddressIp("1.0.0.0");
        PrivatePublicAddressMappingAck ack = new PrivatePublicAddressMappingAck(targetName, targetInternalIp, targetExternalIp);

        DataOutputStreamPlus out = getOutput(BIN);
        PrivatePublicAddressMappingAck.serializer.serialize(ack, out, getVersion());
        out.close();

        DataInputStream in = getInput(BIN);
        PrivatePublicAddressMappingAck deserialized = PrivatePublicAddressMappingAck.serializer.deserialize(in, getVersion());
        in.close();

        assertThat(ack).isEqualTo(deserialized);
        testSerializedSize(ack, PrivatePublicAddressMappingAck.serializer);
    }
}
