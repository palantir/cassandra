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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.shaded.netty.util.internal.ConcurrentSet;
import org.apache.thrift.transport.TTransport;

/**
 * Encapsulates the current client state (session).
 *
 * We rely on the Thrift server to tell us what socket it is
 * executing a request for via setCurrentSocket, after which currentSession can do its job anywhere.
 */
public class ThriftSessionManager
{
    private static final Logger logger = LoggerFactory.getLogger(ThriftSessionManager.class);
    public final static ThriftSessionManager instance = new ThriftSessionManager();

    private final ThreadLocal<SocketAddress> remoteSocket = new ThreadLocal<>();
    private final ConcurrentHashMap<SocketAddress, ThriftClientState> activeSocketSessions = new ConcurrentHashMap<>();
    private final ConcurrentSet<TTransport> activeClients = new ConcurrentSet<>();

    /**
     * @param socket the address on which the current thread will work on requests for until further notice
     */
    public void setCurrentSocket(SocketAddress socket)
    {
        remoteSocket.set(socket);
    }

    public void trackClient(TTransport client)
    {
        activeClients.add(client);
    }

    public void untrackClient(TTransport client) {
        activeClients.remove(client);
    }

    /**
     * @return the current session for the most recently given socket on this thread
     */
    public ThriftClientState currentSession()
    {
        SocketAddress socket = remoteSocket.get();
        assert socket != null;

        ThriftClientState cState = activeSocketSessions.get(socket);
        if (cState == null)
        {
            //guarantee atomicity
            ThriftClientState newState = new ThriftClientState((InetSocketAddress)socket);
            cState = activeSocketSessions.putIfAbsent(socket, newState);
            if (cState == null)
                cState = newState;
        }
        return cState;
    }

    /**
     * The connection associated with @param socket is permanently finished.
     */
    public void connectionComplete(SocketAddress socket)
    {
        assert socket != null;
        activeSocketSessions.remove(socket);
        if (logger.isTraceEnabled())
            logger.trace("ClientState removed for socket addr {}", socket);
    }
    
    public int getConnectedClients()
    {
        return activeSocketSessions.size();
    }

    /**
     * Currently only implemented for synchronous thrift server. This ensures that when the server socket is closed,
     * spawned client sockets will also be closed instead of waiting for TCP timeouts.
     */
    public void closeActiveClients() throws IOException
    {
        Set<TTransport> currentActive = ImmutableSet.copyOf(activeClients);
        currentActive.stream().filter(TTransport::isOpen).forEach(TTransport::close);
        activeClients.removeAll(currentActive);
    }
}
