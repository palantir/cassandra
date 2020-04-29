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

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleSeedProviderTest
{
    private static final String SELF = "127.0.0.1";
    private static final String OTHER = "127.0.0.2";

    private final InetAddress resolvedSelf;

    public SimpleSeedProviderTest() throws UnknownHostException
    {
        resolvedSelf = InetAddress.getByName(SELF);
    }

    @Test
    public void removesSelf_ifOtherSeedsExist_ifClusterIsNew()
    {
        SimpleSeedProvider seedProvider = new SimpleSeedProvider(true);
        List<InetAddress> results = seedProvider.getSeeds(new String[] {SELF, OTHER}, resolvedSelf);
        assertTrue(results.size() == 1);
        assertTrue(results.get(0).getHostAddress().equals(OTHER));
        assertFalse(results.get(0).getHostAddress().equals(SELF));
    }

    @Test
    public void removesSelf_ifOtherSeedsExist_ifClusterIsNotNew()
    {
        SimpleSeedProvider seedProvider = new SimpleSeedProvider(false);
        List<InetAddress> results = seedProvider.getSeeds(new String[] {SELF, OTHER}, resolvedSelf);
        assertTrue(results.size() == 1);
        assertTrue(results.get(0).getHostAddress().equals(OTHER));
        assertFalse(results.get(0).getHostAddress().equals(SELF));
    }

    @Test
    public void returnsSelf_ifClusterIsNew()
    {
        SimpleSeedProvider seedProvider = new SimpleSeedProvider(true);
        List<InetAddress> results = seedProvider.getSeeds(new String[] {SELF}, resolvedSelf);
        assertTrue(results.size() == 1);
        assertTrue(results.get(0).getHostAddress().equals(SELF));
        assertFalse(results.get(0).getHostAddress().equals(OTHER));
    }

    @Test
    public void returnsEmpty_ifClusterIsNotNew()
    {
        SimpleSeedProvider seedProvider = new SimpleSeedProvider(false);
        List<InetAddress> results = seedProvider.getSeeds(new String[] {SELF}, resolvedSelf);
        assertTrue(results.size() == 0);
    }
}
