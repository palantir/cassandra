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

import java.io.DataInput;
import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class CrossVpcIpMappingAck
{
    public static final IVersionedSerializer<CrossVpcIpMappingAck> serializer = new CrossVpcIpMappingAckSerializer();

    private final InetAddressHostname targetHostname;
    private final InetAddressIp targetInternalAddress;

    // target node's resolved IP from source-node's DNS (within other VPC)
    private final InetAddressIp targetExternalAddress;

    public CrossVpcIpMappingAck(@Nonnull InetAddressHostname targetHostname,
                                @Nonnull InetAddressIp targetInternalAddress,
                                @Nonnull InetAddressIp targetExternalAddress)
    {
        this.targetHostname = targetHostname;
        this.targetExternalAddress = targetExternalAddress;
        this.targetInternalAddress = targetInternalAddress;
    }

    public InetAddressHostname getTargetHostname()
    {
        return this.targetHostname;
    }

    public InetAddressIp getTargetExternalAddress()
    {
        return this.targetExternalAddress;
    }

    public InetAddressIp getTargetInternalAddress()
    {
        return this.targetInternalAddress;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof CrossVpcIpMappingAck)) return false;

        CrossVpcIpMappingAck other = (CrossVpcIpMappingAck) o;
        return Objects.equals(this.getTargetHostname(), other.getTargetHostname())
               && Objects.equals(this.getTargetExternalAddress(), other.getTargetExternalAddress())
               && Objects.equals(this.getTargetInternalAddress(), other.getTargetInternalAddress());
    }
}

class CrossVpcIpMappingAckSerializer implements IVersionedSerializer<CrossVpcIpMappingAck>
{
    public void serialize(CrossVpcIpMappingAck ackMessage, DataOutputPlus out, int version) throws IOException
    {
        out.writeUTF(ackMessage.getTargetHostname().toString());
        out.writeUTF(ackMessage.getTargetInternalAddress().toString());
        out.writeUTF(ackMessage.getTargetExternalAddress().toString());
    }

    public CrossVpcIpMappingAck deserialize(DataInput in, int version) throws IOException
    {
        InetAddressHostname targetHostname = new InetAddressHostname(in.readUTF());
        InetAddressIp targetInternalAddress = new InetAddressIp(in.readUTF());
        InetAddressIp targetExternalAddress = new InetAddressIp(in.readUTF());
        return new CrossVpcIpMappingAck(targetHostname, targetInternalAddress, targetExternalAddress);
    }

    public long serializedSize(CrossVpcIpMappingAck ack, int version)
    {
        return TypeSizes.NATIVE.sizeof(ack.getTargetHostname().toString())
               + TypeSizes.NATIVE.sizeof(ack.getTargetInternalAddress().toString())
               + TypeSizes.NATIVE.sizeof(ack.getTargetExternalAddress().toString());
    }
}
