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

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.Callable;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import com.palantir.cassandra.cvim.CrossVpcIpMappingHandshaker;
import com.palantir.cassandra.cvim.InetAddressHostname;
import com.palantir.cassandra.cvim.InetAddressIp;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.AfterClass;
import org.junit.Test;

public class SSLFactoryTest
{
    private static final int PORT = 55123;

    @AfterClass
    public static void after() {
        DatabaseDescriptor.setCrossVpcHostnameSwapping(false);
        DatabaseDescriptor.setCrossVpcIpSwapping(false);
    }

    @Test
    public void connectWithTimeout_interrupts() throws IOException
    {
        Callable<SSLSocket> callable = () -> {
            Thread.sleep(10000);
            return null;
        };
        assertThatThrownBy(() -> SSLFactory.maybeConnectWithTimeout(callable, 100)).isInstanceOf(IOException.class);
    }

    @Test
    public void testFilterCipherSuites()
    {
        String[] supported = new String[]{ "x", "b", "c", "f" };
        String[] desired = new String[]{ "k", "a", "b", "c" };
        assertArrayEquals(new String[]{ "b", "c" }, SSLFactory.filterCipherSuites(supported, desired));

        desired = new String[]{ "c", "b", "x" };
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
    public void getSocket_invokesCrossVpcMaybeSwapAddress_twoEndpoints() throws IOException
    {
        InetAddress localhost = InetAddress.getByName("localhost");
        InetAddress addressToReplace = InetAddress.getByName("10.0.0.1");
        assertThat("127.0.0.1").isNotEqualTo(addressToReplace.getHostAddress());
        assertThat("localhost").isNotEqualTo(addressToReplace.getHostName());

        InetAddressHostname name = new InetAddressHostname(localhost.getHostName());
        InetAddressIp internal = new InetAddressIp(addressToReplace.getHostAddress());
        InetAddressIp external = new InetAddressIp(localhost.getHostAddress());
        // Swap hostname from whatever 10.0.0.1 resolves to with "localhost"
        CrossVpcIpMappingHandshaker.instance.updateCrossVpcMappings(name, internal, external);
        DatabaseDescriptor.setCrossVpcHostnameSwapping(true);
        DatabaseDescriptor.setCrossVpcIpSwapping(false);

        try (SSLServerSocket server = SSLFactory.getServerSocket(getServerEncryptionOptions(), localhost, PORT))
        {
            SSLSocket client = SSLFactory.getSocket(getClientEncryptionOptions(),
                                                    addressToReplace,
                                                    PORT,
                                                    localhost,
                                                    PORT + 1);
            List<SNIServerName> snis = client.getSSLParameters().getServerNames();
            SNIHostName expected = new SNIHostName("localhost");
            assertThat(snis).containsOnly(expected);
            server.close();
            client.close();
        }
    }

    @Test
    public void getSocket_invokesCrossVpcMaybeSwapAddress_oneEndpoint() throws IOException
    {
        InetAddress localhost = InetAddress.getByName("localhost");
        InetAddress addressToReplace = InetAddress.getByName("10.0.0.1");
        assertThat("127.0.0.1").isNotEqualTo(addressToReplace.getHostAddress());
        assertThat("localhost").isNotEqualTo(addressToReplace.getHostName());

        InetAddressHostname name = new InetAddressHostname(localhost.getHostName());
        InetAddressIp internal = new InetAddressIp(addressToReplace.getHostAddress());
        InetAddressIp external = new InetAddressIp(localhost.getHostAddress());
        // Swap hostname from whatever 10.0.0.1 resolves to with "localhost"
        CrossVpcIpMappingHandshaker.instance.updateCrossVpcMappings(name, internal, external);
        DatabaseDescriptor.setCrossVpcHostnameSwapping(true);
        DatabaseDescriptor.setCrossVpcIpSwapping(false);

        try (SSLServerSocket server = SSLFactory.getServerSocket(getServerEncryptionOptions(), localhost, PORT))
        {
            SSLSocket client = SSLFactory.getSocket(getClientEncryptionOptions(), addressToReplace, PORT);
            List<SNIServerName> snis = client.getSSLParameters().getServerNames();
            SNIHostName expected = new SNIHostName("localhost");
            assertThat(snis).containsOnly(expected);
            server.close();
            client.close();
        }
    }

    @Test
    public void getSocket_sniHeadersIfHostnamePresent_oneEndpoint() throws IOException
    {
        InetAddress endpoint = FBUtilities.getLocalAddress();
        assertThat(endpoint.getHostName()).isNotNull();
        try (SSLServerSocket server = SSLFactory.getServerSocket(getServerEncryptionOptions(), endpoint, PORT))
        {
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
        try (SSLServerSocket server = SSLFactory.getServerSocket(getServerEncryptionOptions(), endpoint, PORT))
        {
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
        options.cipher_suites = new String[]{
        "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA",
        "TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"
        };
        return options;
    }
}
