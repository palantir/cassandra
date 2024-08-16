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

package org.apache.cassandra.thrift;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.time.Duration;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class CustomTThreadPoolServerTest
{
    @Test
    public void stop_closesActiveSockets() throws TException, IOException
    {
        InetAddress ip = InetAddress.getLocalHost();
        int port = 9043;
        ThriftServer server = new ThriftServer(ip, port, 10);
        server.start();

        Cassandra.Client clientThatHasSentARequest = getClient(port);
        Cassandra.Client clientThatHasConnectedButNotRequested = getClient(port);

        assertThat(clientThatHasSentARequest.describe_keyspaces()).isNotNull();

        server.stop();

        assertThatThrownBy(() -> getClient(port)).hasCauseInstanceOf(ConnectException.class);
        // Waiting to receive data will result in a read timeout if the client socket is not closed as well
        assertThatThrownBy(clientThatHasConnectedButNotRequested::recv_describe_ring).hasMessageContaining("Socket is closed by peer");
        assertThatThrownBy(clientThatHasSentARequest::recv_describe_ring).hasMessageContaining("Socket is closed by peer");
    }

    private static Cassandra.Client getClient(int port) throws TTransportException
    {
        TTransport tr = new TFramedTransport(new TSocket("localhost", port, 2000));
        TProtocol proto = new TBinaryProtocol(tr);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();
        return client;
    }
}
