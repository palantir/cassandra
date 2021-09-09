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

package com.palantir.cassandra.cvam;

import java.io.DataInputStream;
import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.io.util.DataOutputStreamPlus;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class SerializationsTest extends AbstractSerializationsTester
{
    private static final String BIN = "ppam.CrossVpcIpMapping.bin";

    @Test
    public void testCrossVpcIpMapping_syn() throws IOException
    {
        InetAddressHostname sourceName = new InetAddressHostname("localhost");
        InetAddressIp sourceInternalIp = new InetAddressIp("1.0.0.0");
        InetAddressHostname targetName = new InetAddressHostname("target");
        InetAddressIp targetExternalIp = new InetAddressIp("2.0.0.0");
        CrossVpcIpMappingSyn syn = new CrossVpcIpMappingSyn(sourceName, sourceInternalIp, targetName, targetExternalIp);

        DataOutputStreamPlus out = getOutput(BIN);
        CrossVpcIpMappingSyn.serializer.serialize(syn, out, getVersion());
        out.close();

        DataInputStream in = getInput(BIN);
        CrossVpcIpMappingSyn deserialized = CrossVpcIpMappingSyn.serializer.deserialize(in, getVersion());
        in.close();

        assertThat(syn).isEqualTo(deserialized);

        testSerializedSize(syn, CrossVpcIpMappingSyn.serializer);
    }

    @Test
    public void testCrossVpcIpMapping_ack() throws IOException
    {
        InetAddressHostname targetName = new InetAddressHostname("target");
        InetAddressIp targetExternalIp = new InetAddressIp("2.0.0.0");
        InetAddressIp targetInternalIp = new InetAddressIp("1.0.0.0");
        CrossVpcIpMappingAck ack = new CrossVpcIpMappingAck(targetName, targetInternalIp, targetExternalIp);

        DataOutputStreamPlus out = getOutput(BIN);
        CrossVpcIpMappingAck.serializer.serialize(ack, out, getVersion());
        out.close();

        DataInputStream in = getInput(BIN);
        CrossVpcIpMappingAck deserialized = CrossVpcIpMappingAck.serializer.deserialize(in, getVersion());
        in.close();

        assertThat(ack).isEqualTo(deserialized);
        testSerializedSize(ack, CrossVpcIpMappingAck.serializer);
    }
}
