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
package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

public class MutationVerbHandler implements IVerbHandler<Mutation>
{
    private static final boolean TEST_FAIL_WRITES = System.getProperty("cassandra.test.fail_writes", "false").equalsIgnoreCase("true");
    private static final boolean VERIFY_KEYS_ON_WRITE = Boolean.valueOf(System.getProperty("palantir_cassandra.verify_keys_on_write", "false"));

    private static final Logger logger = LoggerFactory.getLogger(Keyspace.class);

    public void doVerb(MessageIn<Mutation> message, int id)  throws IOException
    {
            // Check if there were any forwarding headers in this message
            byte[] from = message.parameters.get(Mutation.FORWARD_FROM);
            InetAddress replyTo;
            if (from == null)
            {
                replyTo = message.from;
                byte[] forwardBytes = message.parameters.get(Mutation.FORWARD_TO);
                if (forwardBytes != null)
                    forwardToLocalNodes(message.payload, message.verb, forwardBytes, message.from);
            }
            else
            {
                replyTo = InetAddress.getByAddress(from);
            }

            if (VERIFY_KEYS_ON_WRITE) {
                Mutation mutation = message.payload;
                Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
                if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy) {
                    Token tk = StorageService.getPartitioner().getToken(mutation.key());
                    List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(mutation.getKeyspaceName(), tk);
                    Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, mutation.getKeyspaceName());

                    if(!naturalEndpoints.contains(FBUtilities.getBroadcastAddress()) && !pendingEndpoints.contains(FBUtilities.getBroadcastAddress())) {
                        logger.error("Cannot apply mutation as this host {} does not contain key {}. Only hosts {} and {} do.",
                                     FBUtilities.getBroadcastAddress(),
                                     mutation.key(),
                                     naturalEndpoints,
                                     pendingEndpoints);
                        throw new RuntimeException("Cannot apply mutation as this host does not contain key.");
                    }
                }
            }

            message.payload.apply();
            WriteResponse response = new WriteResponse();
            Tracing.trace("Enqueuing response to {}", replyTo);
            MessagingService.instance().sendReply(response.createMessage(), id, replyTo);
    }

    /**
     * Older version (< 1.0) will not send this message at all, hence we don't
     * need to check the version of the data.
     */
    private void forwardToLocalNodes(Mutation mutation, MessagingService.Verb verb, byte[] forwardBytes, InetAddress from) throws IOException
    {
        try (DataInputStream in = new DataInputStream(new FastByteArrayInputStream(forwardBytes)))
        {
            int size = in.readInt();

            // tell the recipients who to send their ack to
            MessageOut<Mutation> message = new MessageOut<>(verb, mutation, Mutation.serializer).withParameter(Mutation.FORWARD_FROM, from.getAddress());
            // Send a message to each of the addresses on our Forward List
            for (int i = 0; i < size; i++)
            {
                InetAddress address = CompactEndpointSerializationHelper.deserialize(in);
                int id = in.readInt();
                Tracing.trace("Enqueuing forwarded write to {}", address);
                MessagingService.instance().sendOneWay(message, id, address);
            }
        }
    }
}
