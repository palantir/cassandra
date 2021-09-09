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

public class CrossVpcIpMappingSyn
{
    public static final IVersionedSerializer<CrossVpcIpMappingSyn> serializer = new CrossVpcIpMappingSynSerializer();

    private final InetAddressHostname sourceHostname;
    private final InetAddressIp sourceInternalAddress;

    private final InetAddressHostname targetHostname;
    private final InetAddressIp targetExternalAddress;

    public CrossVpcIpMappingSyn(@Nonnull InetAddressHostname sourceHostname, @Nonnull InetAddressIp sourceInternalAddress, @Nonnull InetAddressHostname targetHostname, @Nonnull InetAddressIp targetExternalAddress)
    {
        this.sourceHostname = sourceHostname;
        this.sourceInternalAddress = sourceInternalAddress;
        this.targetHostname = targetHostname;
        this.targetExternalAddress = targetExternalAddress;
    }

    public InetAddressHostname getSourceHostname()
    {
        return this.sourceHostname;
    }

    public InetAddressIp getSourceInternalAddress()
    {
        return this.sourceInternalAddress;
    }

    public InetAddressHostname getTargetHostname()
    {
        return this.targetHostname;
    }

    public InetAddressIp getTargetExternalAddress()
    {
        return this.targetExternalAddress;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof CrossVpcIpMappingSyn)) return false;

        CrossVpcIpMappingSyn other = (CrossVpcIpMappingSyn) o;
        return Objects.equals(this.getSourceHostname(), other.getSourceHostname())
                   && Objects.equals(this.getSourceInternalAddress(), other.getSourceInternalAddress())
                   && Objects.equals(this.getTargetHostname(), other.getTargetHostname())
                   && Objects.equals(this.getTargetExternalAddress(), other.getTargetExternalAddress());
    }
}

class CrossVpcIpMappingSynSerializer
implements IVersionedSerializer<CrossVpcIpMappingSyn>
{
    public void serialize(CrossVpcIpMappingSyn synMessage, DataOutputPlus out, int version) throws IOException
    {
        out.writeUTF(synMessage.getSourceHostname().toString());
        out.writeUTF(synMessage.getSourceInternalAddress().toString());
        out.writeUTF(synMessage.getTargetHostname().toString());
        out.writeUTF(synMessage.getTargetExternalAddress().toString());
    }

    public CrossVpcIpMappingSyn deserialize(DataInput in, int version) throws IOException
    {
        InetAddressHostname sourceHostname = new InetAddressHostname(in.readUTF());
        InetAddressIp sourceInternalAddress = new InetAddressIp(in.readUTF());
        InetAddressHostname targetHostname = new InetAddressHostname(in.readUTF());
        InetAddressIp targetExternalAddress = new InetAddressIp(in.readUTF());
        return new CrossVpcIpMappingSyn(sourceHostname, sourceInternalAddress, targetHostname, targetExternalAddress);
    }

    public long serializedSize(CrossVpcIpMappingSyn syn, int version)
    {
        return TypeSizes.NATIVE.sizeof(syn.getSourceHostname().toString())
               + TypeSizes.NATIVE.sizeof(syn.getSourceInternalAddress().toString())
               + TypeSizes.NATIVE.sizeof(syn.getTargetHostname().toString())
               + TypeSizes.NATIVE.sizeof(syn.getTargetExternalAddress().toString());
    }
}
