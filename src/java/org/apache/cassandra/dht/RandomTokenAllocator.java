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

package org.apache.cassandra.dht;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.logsafe.SafeArg;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;

public class RandomTokenAllocator implements ITokenAllocator
{
    private static final Logger logger = LoggerFactory.getLogger(RandomTokenAllocator.class);

    public Collection<Token> allocateTokens(TokenMetadata metadata, int numTokens)
    {
        Set<Token> tokens = new HashSet<>(numTokens);
        while (tokens.size() < numTokens)
        {
            Token token = StorageService.getPartitioner().getRandomToken();
            if (metadata.getEndpoint(token) == null)
                tokens.add(token);
        }

        if (DatabaseDescriptor.getNumTokens() == 1)
            logger.warn("Generated single random token {}. Random tokens will result in an unbalanced ring; see http://wiki.apache.org/cassandra/Operations",
                        SafeArg.of("token", tokens));
        else
            logger.info("Generated random tokens. tokens are {}", SafeArg.of("tokens", tokens));

        return tokens;
    }
}
