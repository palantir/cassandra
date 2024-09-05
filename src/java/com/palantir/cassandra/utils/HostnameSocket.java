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

package com.palantir.cassandra.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketImplFactory;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;

public class HostnameSocket extends Socket
{
    private final Socket delegate;
    private final InetAddress inetAddress;

    public HostnameSocket(Socket delegate, String hostname)
    {
        this.delegate = delegate;
        try
        {
            inetAddress = InetAddress.getByAddress(hostname, delegate.getInetAddress().getAddress());
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void connect(SocketAddress endpoint) throws IOException
    {
        delegate.connect(endpoint);
    }

    public void connect(SocketAddress endpoint, int timeout) throws IOException
    {
        delegate.connect(endpoint, timeout);
    }

    public void bind(SocketAddress bindpoint) throws IOException
    {
        delegate.bind(bindpoint);
    }

    public InetAddress getInetAddress()
    {
        return inetAddress;
    }

    public InetAddress getLocalAddress()
    {
        return delegate.getLocalAddress();
    }

    public int getPort()
    {
        return delegate.getPort();
    }

    public int getLocalPort()
    {
        return delegate.getLocalPort();
    }

    public SocketAddress getRemoteSocketAddress()
    {
        return delegate.getRemoteSocketAddress();
    }

    public SocketAddress getLocalSocketAddress()
    {
        return delegate.getLocalSocketAddress();
    }

    public SocketChannel getChannel()
    {
        return delegate.getChannel();
    }

    public InputStream getInputStream() throws IOException
    {
        return delegate.getInputStream();
    }

    public OutputStream getOutputStream() throws IOException
    {
        return delegate.getOutputStream();
    }

    public void setTcpNoDelay(boolean on) throws SocketException
    {
        delegate.setTcpNoDelay(on);
    }

    public boolean getTcpNoDelay() throws SocketException
    {
        return delegate.getTcpNoDelay();
    }

    public void setSoLinger(boolean on, int linger) throws SocketException
    {
        delegate.setSoLinger(on, linger);
    }

    public int getSoLinger() throws SocketException
    {
        return delegate.getSoLinger();
    }

    public void sendUrgentData(int data) throws IOException
    {
        delegate.sendUrgentData(data);
    }

    public void setOOBInline(boolean on) throws SocketException
    {
        delegate.setOOBInline(on);
    }

    public boolean getOOBInline() throws SocketException
    {
        return delegate.getOOBInline();
    }

    public void setSoTimeout(int timeout) throws SocketException
    {
        delegate.setSoTimeout(timeout);
    }

    public int getSoTimeout() throws SocketException
    {
        return delegate.getSoTimeout();
    }

    public void setSendBufferSize(int size) throws SocketException
    {
        delegate.setSendBufferSize(size);
    }

    public int getSendBufferSize() throws SocketException
    {
        return delegate.getSendBufferSize();
    }

    public void setReceiveBufferSize(int size) throws SocketException
    {
        delegate.setReceiveBufferSize(size);
    }

    public int getReceiveBufferSize() throws SocketException
    {
        return delegate.getReceiveBufferSize();
    }

    public void setKeepAlive(boolean on) throws SocketException
    {
        delegate.setKeepAlive(on);
    }

    public boolean getKeepAlive() throws SocketException
    {
        return delegate.getKeepAlive();
    }

    public void setTrafficClass(int tc) throws SocketException
    {
        delegate.setTrafficClass(tc);
    }

    public int getTrafficClass() throws SocketException
    {
        return delegate.getTrafficClass();
    }

    public void setReuseAddress(boolean on) throws SocketException
    {
        delegate.setReuseAddress(on);
    }

    public boolean getReuseAddress() throws SocketException
    {
        return delegate.getReuseAddress();
    }

    public void close() throws IOException
    {
        delegate.close();
    }

    public void shutdownInput() throws IOException
    {
        delegate.shutdownInput();
    }

    public void shutdownOutput() throws IOException
    {
        delegate.shutdownOutput();
    }

    public String toString()
    {
        return delegate.toString();
    }

    public boolean isConnected()
    {
        return delegate.isConnected();
    }

    public boolean isBound()
    {
        return delegate.isBound();
    }

    public boolean isClosed()
    {
        return delegate.isClosed();
    }

    public boolean isInputShutdown()
    {
        return delegate.isInputShutdown();
    }

    public boolean isOutputShutdown()
    {
        return delegate.isOutputShutdown();
    }

    public void setPerformancePreferences(int connectionTime, int latency, int bandwidth)
    {
        delegate.setPerformancePreferences(connectionTime, latency, bandwidth);
    }
}
