/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.security;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.List;

import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

public class SSLFactoryTest
{
    private static final int PORT = 55123;

    @Test
    public void testFilterCipherSuites()
    {
        String[] supported = new String[] {"x", "b", "c", "f"};
        String[] desired = new String[] { "k", "a", "b", "c" };
        assertArrayEquals(new String[] { "b", "c" }, SSLFactory.filterCipherSuites(supported, desired));

        desired = new String[] { "c", "b", "x" };
        assertArrayEquals(desired, SSLFactory.filterCipherSuites(supported, desired));
    }

    @Test
    public void testServerSocketCiphers() throws IOException
    {
        EncryptionOptions options = getServerEncryptionOptions();

        // enabled ciphers must be a subset of configured ciphers with identical order
        try (SSLServerSocket socket = SSLFactory.getServerSocket(options, FBUtilities.getLocalAddress(), PORT))
        {
            String[] enabled = socket.getEnabledCipherSuites();
            String[] wanted = Iterables.toArray(Iterables.filter(Lists.newArrayList(options.cipher_suites),
                                                                 Predicates.in(Lists.newArrayList(enabled))),
                                                String.class);
            assertArrayEquals(wanted, enabled);
        }
    }

    @Test
    public void getSocket_sniHeadersIfHostnamePresent_oneEndpoint() throws IOException
    {
        InetAddress endpoint = FBUtilities.getLocalAddress();
        assertThat(endpoint.getHostName()).isNotNull();
        try (SSLServerSocket server = SSLFactory.getServerSocket(getServerEncryptionOptions(), endpoint, PORT)) {
            SSLSocket client = SSLFactory.getSocket(getClientEncryptionOptions(), endpoint, PORT);
            assertThat(client.getSSLParameters().getServerNames()).isNotEmpty();
            server.close();
            client.close();
        }
    }

    @Test
    public void getSocket_noSniHeadersIfHostnameAbsent_oneEndpoint() throws IOException
    {
        InetAddress endpoint = FBUtilities.getLocalAddress();
        assertThat(endpoint.getHostName()).isNotNull();
        try (SSLServerSocket server = SSLFactory.getServerSocket(getServerEncryptionOptions(), endpoint, PORT)) {
            SSLSocket client = SSLFactory.getSocket(getClientEncryptionOptions(), endpoint, PORT);
            assertThat(client.getSSLParameters().getServerNames()).isNotEmpty();
            server.close();
            client.close();
        }
    }

    private static EncryptionOptions getClientEncryptionOptions()
    {
        EncryptionOptions options = new EncryptionOptions.ClientEncryptionOptions();
        return setOptions(options);
    }

    private static EncryptionOptions getServerEncryptionOptions()
    {
        EncryptionOptions options = new EncryptionOptions.ServerEncryptionOptions();
        return setOptions(options);
    }

    private static EncryptionOptions setOptions(EncryptionOptions options)
    {
        options.keystore = "test/conf/keystore.jks";
        options.keystore_password = "cassandra";
        options.truststore = options.keystore;
        options.truststore_password = options.keystore_password;
        options.require_endpoint_verification = true;
        options.cipher_suites = new String[] {
        "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA",
        "TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"
        };
        return options;
    }
}
