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

package org.apache.cassandra.distributed.test;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AbstractFuture;
import org.junit.Assert;
import org.junit.Test;

import com.palantir.logsafe.exceptions.SafeRuntimeException;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.gms.EchoMessage;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public final class MessagingServiceConnectionResetTest extends TestBaseImpl
{
    @Test
    public void echoRequestResponse() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(config -> config.with(NETWORK, NATIVE_PROTOCOL))
                                             .start()))
        {
            InetAddress anotherNode = cluster.get(2).broadcastAddress().getAddress();
            cluster.get(1).runOnInstance(() -> {
                MessageOut<EchoMessage> echoMessage = new MessageOut<>(MessagingService.Verb.ECHO, EchoMessage.instance, EchoMessage.serializer);
                FutureCallback<EchoMessage> res = FutureCallback.create();
                MessagingService.instance().sendRRWithFailure(echoMessage, anotherNode, res);
                MessageIn<EchoMessage> echoMessageMessageIn = FBUtilities.waitOnFuture(res);
                Assert.assertEquals(echoMessageMessageIn.from, anotherNode);
            });
        }
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
}
