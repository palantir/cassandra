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
package org.apache.cassandra.security;


import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.palantir.cassandra.cvim.CrossVpcIpMappingHandshaker;
import com.palantir.cassandra.cvim.InetAddressHostname;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.io.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * A Factory for providing and setting up Client and Server SSL wrapped
 * Socket and ServerSocket
 */
public final class SSLFactory
{
    private static final Logger logger = LoggerFactory.getLogger(SSLFactory.class);

    // Clients will send SSLv2Hello `Client Hello` messages during the SSL/TLS handshake to initiate SSL/TLS version
    // negotiation with the server. We want to get rid of SSLv2Hello because it does not support SNI headers, but
    // removing support entirely causes a breaking change while upgrading (the upgraded node will reject the
    // initial SSLv2Hello message from the non-upgraded nodes, failing to connect and causing downtime). Consequently
    // we stop only clients from sending SSLv2Hello messages (and may later stop servers from accepting them as well).
    public static final String[] ACCEPTED_INCOMING_PROTOCOLS = new String[]{ "SSLv2Hello", "TLSv1", "TLSv1.1", "TLSv1.2"};
    public static final String[] ACCEPTED_OUTGOING_PROTOCOLS = new String[]{ "TLSv1", "TLSv1.1", "TLSv1.2"};

    private static boolean checkedExpiry = false;

    public static SSLServerSocket getServerSocket(EncryptionOptions options, InetAddress address, int port) throws IOException
    {
        SSLContext ctx = createSSLContext(options, true);
        SSLServerSocket serverSocket = (SSLServerSocket)ctx.getServerSocketFactory().createServerSocket();
        serverSocket.setReuseAddress(true);
        prepareSocket(serverSocket, options);
        serverSocket.bind(new InetSocketAddress(address, port), 500);
        return serverSocket;
    }

    /** Create a socket and connect */
    public static SSLSocket getSocket(EncryptionOptions options, InetAddress address, int port, InetAddress localAddress, int localPort) throws Exception
    {
        SSLContext ctx = createSSLContext(options, true);
        InetAddress mappedAddress = CrossVpcIpMappingHandshaker.instance.maybeSwapAddress(address);
        SSLSocket socket = maybeConnectWithTimeout(() -> (SSLSocket) ctx.getSocketFactory().createSocket(mappedAddress, port, localAddress, localPort));
        prepareSocket(socket, options, mappedAddress);
        return socket;
    }

    /** Create a socket and connect, using any local address */
    public static SSLSocket getSocket(EncryptionOptions options, InetAddress address, int port) throws Exception
    {
        SSLContext ctx = createSSLContext(options, true);
        InetAddress mappedAddress = CrossVpcIpMappingHandshaker.instance.maybeSwapAddress(address);
        SSLSocket socket = maybeConnectWithTimeout(() -> (SSLSocket) ctx.getSocketFactory().createSocket(mappedAddress, port));
        prepareSocket(socket, options, mappedAddress);
        return socket;
    }

    /** Just create a socket. Note that no SNI headeers are added. */
    public static SSLSocket getSocket(EncryptionOptions options) throws IOException
    {
        SSLContext ctx = createSSLContext(options, true);
        SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket();
        prepareSocket(socket, options);
        return socket;
    }

    @SuppressWarnings("resource")
    public static SSLContext createSSLContext(EncryptionOptions options, boolean buildTruststore) throws IOException
    {
        FileInputStream tsf = null;
        FileInputStream ksf = null;
        SSLContext ctx;
        try
        {
            logger.trace("Creating socket with protocol {}", options.protocol);
            ctx = SSLContext.getInstance(options.protocol);
            TrustManager[] trustManagers = null;

            if(buildTruststore)
            {
                tsf = new FileInputStream(options.truststore);
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(options.algorithm);
                KeyStore ts = KeyStore.getInstance(options.store_type);
                ts.load(tsf, options.truststore_password.toCharArray());
                tmf.init(ts);
                trustManagers = tmf.getTrustManagers();
            }

            ksf = new FileInputStream(options.keystore);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(options.algorithm);
            KeyStore ks = KeyStore.getInstance(options.store_type);
            ks.load(ksf, options.keystore_password.toCharArray());
            if (!checkedExpiry)
            {
                for (Enumeration<String> aliases = ks.aliases(); aliases.hasMoreElements(); )
                {
                    String alias = aliases.nextElement();
                    if (ks.getCertificate(alias).getType().equals("X.509"))
                    {
                        Date expires = ((X509Certificate) ks.getCertificate(alias)).getNotAfter();
                        if (expires.before(new Date()))
                            logger.warn("Certificate for {} expired on {}", alias, expires);
                    }
                }
                checkedExpiry = true;
            }
            kmf.init(ks, options.keystore_password.toCharArray());

            ctx.init(kmf.getKeyManagers(), trustManagers, null);
        }
        catch (Exception e)
        {
            throw new IOException("Error creating the initializing the SSL Context", e);
        }
        finally
        {
            FileUtils.closeQuietly(tsf);
            FileUtils.closeQuietly(ksf);
        }
        return ctx;
    }

    public static String[] filterCipherSuites(String[] supported, String[] desired)
    {
        List<String> ldesired = Arrays.asList(desired);
        ImmutableSet<String> ssupported = ImmutableSet.copyOf(supported);

        String[] ret = Iterables.toArray(
        ldesired.stream()
                .filter(potentialCipher -> ssupported.contains(potentialCipher)
                                           && !EncryptionOptions.restricted_cipher_suites.contains(potentialCipher))
                .collect(Collectors.toList()),
        String.class);


        if (desired.length > ret.length && logger.isWarnEnabled())
        {
            Iterable<String> missing = Iterables.filter(ldesired, Predicates.not(Predicates.in(Sets.newHashSet(ret))));
            logger.warn("Filtering out {} as it isn't supported by the socket", Iterables.toString(missing));
        }
        return ret;
    }

    private static SSLSocket maybeConnectWithTimeout(Callable<SSLSocket> callable) throws Exception
    {
        return maybeConnectWithTimeout(callable, DatabaseDescriptor.getCrossVpcConnectTimeoutMs());
    }

    @VisibleForTesting
    static SSLSocket maybeConnectWithTimeout(Callable<SSLSocket> callable, long timeout) throws Exception
    {
        if (!DatabaseDescriptor.isCrossVpcInternodeCommunicationEnabled()) {
            return callable.call();
        }
        try {
            // Executor allows the Future to actually interrupt the Socket creation call
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<SSLSocket> socket = executor.submit(callable);
            return socket.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(String.format("Failed to create socket after %d millis", timeout), e);
        }
    }

    private static void maybeAddSni(InetAddress addr, SSLParameters sslParameters)
    {
        String providedName = addr.getHostName();
        Optional<InetAddressHostname> maybeName = Optional.empty();
        if (DatabaseDescriptor.isCrossVpcSniSubstitutionEnabled())
        {
            maybeName = CrossVpcIpMappingHandshaker.instance.getAssociatedHostname(addr);
            maybeName.map(InetAddressHostname::toString).filter(swap -> !swap.equals(providedName)).ifPresent(name -> logger.trace("Providing new hostname for SNI header {}->{}", addr, name));
        }
        String name = maybeName.map(InetAddressHostname::toString).orElse(providedName);

        logger.trace(
            "Adding SNI header to socket if hostname present for {}/{}", name, addr.getHostAddress());
        if (name != null)
        {
            SNIServerName sniName = new SNIHostName(name);
            List<SNIServerName> sniHostNames = ImmutableList.of(sniName);
            sslParameters.setServerNames(sniHostNames);
        }
    }

    /** Sets relevant socket options specified in encryption settings */
    private static void prepareSocket(SSLServerSocket serverSocket, EncryptionOptions options)
    {
        String[] suites = filterCipherSuites(serverSocket.getSupportedCipherSuites(), options.cipher_suites);
        if(options.require_endpoint_verification)
        {
            SSLParameters sslParameters = serverSocket.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            serverSocket.setSSLParameters(sslParameters);
        }
        serverSocket.setEnabledCipherSuites(suites);
        serverSocket.setNeedClientAuth(options.require_client_auth);
        serverSocket.setEnabledProtocols(ACCEPTED_INCOMING_PROTOCOLS);
    }

    /** Sets relevant socket options specified in encryption settings */
    private static void prepareSocket(SSLSocket socket, EncryptionOptions options)
    {
        String[] suites = filterCipherSuites(socket.getSupportedCipherSuites(), options.cipher_suites);
        if (options.require_endpoint_verification)
        {
            SSLParameters sslParameters = socket.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            socket.setSSLParameters(sslParameters);
        }
        socket.setEnabledCipherSuites(suites);
        socket.setEnabledProtocols(ACCEPTED_OUTGOING_PROTOCOLS);
    }

    /**
     * Sets relevant socket options specified in encryption settings. May add SNI headers to the socket's SSLParameters
     * if the given endpoint includes a hostname.
     */
    @VisibleForTesting
    static void prepareSocket(SSLSocket socket, EncryptionOptions options, InetAddress endpoint)
    {
        prepareSocket(socket, options);
        SSLParameters sslParameters = socket.getSSLParameters();
        maybeAddSni(endpoint, sslParameters);
        socket.setSSLParameters(sslParameters);
    }
}
