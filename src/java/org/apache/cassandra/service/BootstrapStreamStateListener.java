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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressListener;
import org.apache.hadoop.util.Progress;

public class BootstrapStreamStateListener implements ProgressListener
{
    private static final Logger logger = LoggerFactory.getLogger(BootstrapStreamStateListener.class);

    private final AtomicBoolean success = new AtomicBoolean(false);
    private final AtomicBoolean failure = new AtomicBoolean(false);
    private final Map<InetAddress, String> state = new ConcurrentHashMap<>();

    public enum Status { STARTED, SUCCEDED, FAILED, NONE }

    void reset() {
        success.set(false);
        failure.set(false);
        state.clear();
    }

    Status getStatus() {
        if (failure.get()) {
            return Status.FAILED;
        }
        if (success.get()) {
            return Status.SUCCEDED;
        }
        if (state.isEmpty()) {
            return Status.NONE;
        }
        return Status.STARTED;
    }

    Map<InetAddress, String> getStreamState() {
        return ImmutableMap.copyOf(state);
    }

    private void handleProgressEvent(ProgressEvent event)
    {
        String preparePeer = StringUtils.substringBetween(event.getMessage(), "prepare with ", " complete");
        if (preparePeer != null) {
            InetAddress preparePeerEndpoint;
            try {
                preparePeerEndpoint = InetAddress.getByName(preparePeer);
            }
            catch (UnknownHostException e) {
                logger.error("Unable to find endpoint for bootstrap stream peer {}", preparePeer);
                throw new SafeRuntimeException(e);
            }
            handlePrepareEvent(event, preparePeerEndpoint);
        }
        String completePeer = StringUtils.substringBetween(event.getMessage(), "session with ", " complete");
        if (completePeer != null) {
            InetAddress completePeerEndpoint;
            try {
                completePeerEndpoint = InetAddress.getByName(preparePeer);
            }
            catch (UnknownHostException e) {
                logger.error("Unable to find endpoint for bootstrap stream peer {}", preparePeer);
                throw new SafeRuntimeException(e);
            }
            handleSessionCompleteEvent(event, completePeerEndpoint);
        }
    }

    private void handlePrepareEvent(ProgressEvent event, InetAddress peer) {
        if (state.containsKey(peer)) {
            logger.error("Encountered a duplicate bootstrap stream for endpoint {}", SafeArg.of("endpoint", peer));
            failure.set(true);
        } else {
            state.put(
            peer,
            "prepared");
        }
    }

    private void handleSessionCompleteEvent(ProgressEvent event, InetAddress peer) {
        logger.info("Bootstrap session has completed with peer {}", SafeArg.of("peer", peer));
        state.put(
        peer,
        "completed");
    }

    private void handleSuccessEvent(ProgressEvent event) {
        logger.info("Bootstrap has succeeded: {}", SafeArg.of("event", event));
        success.set(true);
    }

    private void handleErrorEvent(ProgressEvent event) {
        logger.error("Bootstrap has failed: {}", SafeArg.of("event", event));
        failure.set(true);
    }


    @Override
    public void progress(String tag, ProgressEvent event)
    {
        Preconditions.checkArgument(tag.equals("bootstrap"), "Received progress event of incorrect tag.");

        switch (event.getType()) {
            case PROGRESS:
                handleProgressEvent(event);
                break;
            case SUCCESS:
                handleSuccessEvent(event);
                break;
            case ERROR:
                handleErrorEvent(event);
                break;
            default:
                logger.warn("Received bootstrap ProgressEvent of unknown type {}", UnsafeArg.of("progressEvent", event));
                break;
        }
    }
}
