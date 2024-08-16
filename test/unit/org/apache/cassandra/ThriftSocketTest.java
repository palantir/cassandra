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

package org.apache.cassandra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TCustomServerSocket;
import org.apache.cassandra.thrift.TServerCustomFactory;
import org.apache.cassandra.thrift.TServerFactory;
import org.apache.cassandra.thrift.ThriftServer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ThriftSocketTest
{

    @Test
    public void connectionHangs() throws TException, IOException
    {
        InetAddress ip = InetAddress.getLocalHost();
        int port = 9043;
        ThriftServer server = new ThriftServer(ip, port, 10);
        server.start();
        Cassandra.Client clientThatHasSentARequest = getClient(port, (int) Duration.ofSeconds(2).toMillis());
        assertClientTimesOut(clientThatHasSentARequest);

        Cassandra.Client clientThatHasConnectedButNotRequested = getClient(port, (int) Duration.ofSeconds(5).toMillis());

        server.stop();
        assertThatThrownBy(() -> getClient(port, 1000)).hasCauseInstanceOf(ConnectException.class);

        assertThatThrownBy(clientThatHasConnectedButNotRequested::describe_keyspaces).hasCauseInstanceOf(SocketException.class).hasMessageContaining("Connection reset");
        // We would want this to be a socket closed/reset exception
        assertClientDoesNotTimeOut(clientThatHasSentARequest);
    }

    private void assertClientTimesOut(Cassandra.Client client) {
        try {
            client.recv_describe_ring();
        } catch (Exception e) {
            assertThat(e.getCause()).isInstanceOf(SocketTimeoutException.class);
        }
    }

    private void assertClientDoesNotTimeOut(Cassandra.Client client) {
        try {
            client.recv_describe_ring();
        } catch (Exception e) {
            assertThat(e.getCause()).isNotInstanceOf(SocketTimeoutException.class);
        }
    }


    protected static Cassandra.Client getClient(int port, int timeout) throws TTransportException
    {
        TTransport tr = new TFramedTransport(new TSocket("localhost", port, timeout));
        TProtocol proto = new TBinaryProtocol(tr);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();
        return client;
    }

}
