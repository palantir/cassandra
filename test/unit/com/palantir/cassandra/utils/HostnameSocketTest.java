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

import java.net.InetAddress;
import java.net.Socket;

import com.google.common.net.InetAddresses;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class HostnameSocketTest
{
    @Test
    public void testInetAddress()
    {
        InetAddress inetAddress = InetAddresses.forString("10.0.0.1");
        Socket socket = mock(Socket.class);
        doReturn(inetAddress).when(socket).getInetAddress();

        HostnameSocket hostnameSocket = new HostnameSocket(socket, "manually.specified.hostname");
        assertThat(hostnameSocket.getInetAddress().toString()).isEqualTo("manually.specified.hostname/10.0.0.1");
    }
}
