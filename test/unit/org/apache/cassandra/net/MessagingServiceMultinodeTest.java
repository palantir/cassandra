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

package org.apache.cassandra.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.AbstractFuture;
import org.junit.Test;

import org.apache.cassandra.gms.EchoMessage;
import org.apache.cassandra.service.EchoVerbHandler;

public final class MessagingServiceMultinodeTest
{

    private static final FixedDatabaseDescriptor SERVER1;
    private static final FixedDatabaseDescriptor SERVER2;

    static
    {
        SERVER1 = FixedDatabaseDescriptor.create(127, 0, 0, 1);
        SERVER2 = FixedDatabaseDescriptor.create(127, 0, 0, 2);
    }

    @Test
    public void testEchoRequestResponse() throws ExecutionException, InterruptedException
    {

        // Note: server1 is only setup to receive responses, server 2 is only setup to receive requests.pushM
        MessagingService server1 = MessagingService.test(SERVER1);
        server1.registerVerbHandlers(MessagingService.Verb.REQUEST_RESPONSE, ResponseVerbHandler::new);
        server1.listen();
        MessagingService server2 = MessagingService.test(SERVER2);
        server2.registerVerbHandlers(MessagingService.Verb.ECHO, EchoVerbHandler::new);
        server2.listen();

        MessageOut<EchoMessage> echoMessage = new MessageOut<>(MessagingService.Verb.ECHO, EchoMessage.instance, EchoMessage.serializer);

        FutureCallback<EchoMessage> res = FutureCallback.create();
        server1.sendRRWithFailure(echoMessage, SERVER2.address, res);

        res.get();
    }

    private static final class FutureCallback<V> extends AbstractFuture<MessageIn<V>> implements IAsyncCallbackWithFailure<V>
    {

        public void onFailure(InetAddress from)
        {
            setException(new RuntimeException("Failed to send message to " + from));
        }

        public void response(MessageIn<V> msg)
        {
            set(msg);
        }

        public boolean isLatencyForSnitch()
        {
            return true;
        }

        static <V> FutureCallback<V> create()
        {
            return new FutureCallback<V>();
        }
    }

    private static final class FixedDatabaseDescriptor implements IDatabaseDescriptor
    {

        private final InetAddress address;

        private FixedDatabaseDescriptor(InetAddress address)
        {
            this.address = address;
        }

        public InetAddress getBroadcastRpcAddress()
        {
            return address;
        }

        public InetAddress getLocalAddress()
        {
            return address;
        }

        public InetAddress getBroadcastAddress()
        {
            return address;
        }

        public static FixedDatabaseDescriptor create(int a1, int a2, int a3, int a4)
        {
            try
            {
                return new FixedDatabaseDescriptor(InetAddress.getByAddress(new byte[]{ (byte) a1, (byte) a2, (byte) a3, (byte) a4 }));
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
