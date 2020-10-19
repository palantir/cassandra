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

package org.apache.cassandra.db.commitlog;

import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;
import org.assertj.core.api.Assertions;


public class CommitLogFailurePolicyTest
{
    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void testCommitFailurePolicy_stop() throws ConfigurationException
    {
        CassandraDaemon daemon = new CassandraDaemon();
        daemon.completeSetup(); //startup must be completed, otherwise commit log failure must kill JVM regardless of failure policy
        StorageService.instance.registerDaemon(daemon);

        // Need storage service active so stop policy can shutdown gossip
        StorageService.instance.initServer();
        Assert.assertTrue(Gossiper.instance.isEnabled());

        Config.CommitFailurePolicy oldPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.stop);
            CommitLog.handleCommitError("Test stop error", new Throwable());
            Assert.assertFalse(Gossiper.instance.isEnabled());
        }
        finally
        {
            DatabaseDescriptor.setCommitFailurePolicy(oldPolicy);
        }
    }

    @Test
    public void testCommitFailurePolicy_die()
    {
        CassandraDaemon daemon = new CassandraDaemon();
        daemon.completeSetup(); //startup must be completed, otherwise commit log failure must kill JVM regardless of failure policy
        StorageService.instance.registerDaemon(daemon);

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        Config.CommitFailurePolicy oldPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.die);
            CommitLog.handleCommitError("Testing die policy", new Throwable());
            Assert.assertTrue(killerForTests.wasKilled());
            Assert.assertFalse(killerForTests.wasKilledQuietly()); //only killed quietly on startup failure
        }
        finally
        {
            DatabaseDescriptor.setCommitFailurePolicy(oldPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }

    @Test
    public void testCommitFailurePolicy_ignore_beforeStartup()
    {
        //startup was not completed successfuly (since method completeSetup() was not called)
        CassandraDaemon daemon = new CassandraDaemon();
        StorageService.instance.registerDaemon(daemon);

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        Config.CommitFailurePolicy oldPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.ignore);
            CommitLog.handleCommitError("Testing ignore policy", new Throwable());
            //even though policy is ignore, JVM must die because Daemon has not finished initializing
            Assert.assertTrue(killerForTests.wasKilled());
            Assert.assertTrue(killerForTests.wasKilledQuietly()); //killed quietly due to startup failure
        }
        finally
        {
            DatabaseDescriptor.setCommitFailurePolicy(oldPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }

    @Test
    public void testCommitFailurePolicy_ignore_afterStartup() throws Exception
    {
        CassandraDaemon daemon = new CassandraDaemon();
        daemon.completeSetup(); //startup must be completed, otherwise commit log failure must kill JVM regardless of failure policy
        StorageService.instance.registerDaemon(daemon);

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        Config.CommitFailurePolicy oldPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.ignore);
            CommitLog.handleCommitError("Testing ignore policy", new Throwable());
            //error policy is set to IGNORE, so JVM must not be killed if error ocurs after startup
            Assert.assertFalse(killerForTests.wasKilled());
        }
        finally
        {
            DatabaseDescriptor.setCommitFailurePolicy(oldPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }

    @Test
    public void testCommitFailurePolicy_stop_on_startup_beforeStartup()
    {
        //startup was not completed successfuly (since method completeSetup() was not called)
        CassandraDaemon daemon = new CassandraDaemon();
        StorageService.instance.registerDaemon(daemon);

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        Config.CommitFailurePolicy oldPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.stop_on_startup);
            CommitLog.handleCommitError("Testing stop_on_startup policy", new Throwable());
            String commitLogName = "CommitLog.log";
            CommitLog.handleCommitError("Testing stop_on_startup policy with path", new Throwable(), getResolvedCommitLogFilePath(commitLogName));

            Set<Map<String, String>> expectedErrors =
                addCommitLogCorruptionAttribute(ImmutableSet.of(ImmutableMap.of(), ImmutableMap.of("path", commitLogName)));
            Assertions.assertThat(StorageService.instance.getNonTransientErrors()).isEqualTo(expectedErrors);
            //policy is stop_on_startup, JVM shouldn't die even if cassandra wasn't succesfully initialized
            Assert.assertFalse(killerForTests.wasKilled());
        }
        finally
        {
            DatabaseDescriptor.setCommitFailurePolicy(oldPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }

    @Test
    public void testCommitFailurePolicy_stop_on_startup_afterStartup()
    {
        CassandraDaemon daemon = new CassandraDaemon();
        StorageService.instance.registerDaemon(daemon);
        daemon.completeSetup(); //startup completed

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        Config.CommitFailurePolicy oldPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.stop_on_startup);
            CommitLog.handleCommitError("Testing stop_on_startup policy", new Throwable());
            String commitLogName = "CommitLog.log";
            CommitLog.handleCommitError("Testing stop_on_startup policy with path", new Throwable(), getResolvedCommitLogFilePath(commitLogName));

            Set<Map<String, String>> expectedErrors =
                addCommitLogCorruptionAttribute(ImmutableSet.of(ImmutableMap.of(), ImmutableMap.of("path", commitLogName)));
            Assertions.assertThat(StorageService.instance.getNonTransientErrors()).isEqualTo(expectedErrors);
            //error policy is set to stop_on_startup, so JVM must not be killed if error ocurs after startup
            Assert.assertFalse(killerForTests.wasKilled());
        }
        finally
        {
            DatabaseDescriptor.setCommitFailurePolicy(oldPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }

    private String getResolvedCommitLogFilePath(String commitLogName)
    {
        return Paths.get(DatabaseDescriptor.getCommitLogLocation()).resolve(commitLogName).toString();
    }

    private Set<Map<String, String>> addCommitLogCorruptionAttribute(Set<Map<String, String>> errors)
    {
        return errors.stream()
                     .map(error -> ImmutableMap.<String, String>builder()
                            .putAll(error)
                            .put(StorageServiceMBean.NON_TRANSIENT_ERROR_TYPE_KEY, StorageServiceMBean.NonTransientError.COMMIT_LOG_CORRUPTION.toString())
                            .build())
                     .collect(Collectors.toSet());
    }
}
