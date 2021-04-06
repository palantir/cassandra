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

package com.palantir.cassandra.db;

import org.apache.cassandra.db.SystemKeyspace;

/**
 * There is a risk of a node crashing during the bootstrapping process. On the next start the node will detect
 * it has already started to bootstrap via {@link SystemKeyspace#bootstrapInProgress()}. Continuing to bootstrap
 * after a previous failure poses risk of filling up the node's storage device(s). To prevent this a system var can
 * be supplied in the form of <code>palantir_cassandra.bootstrap_disk_usage_threshold_percentage</code> that represents
 * a threshold of how filled data dirs can be, before bootstrapping is prevented if previous failures are detected.
 *
 * If current data dir utilization exceeds the safety threshold, the <code>BootstrappingSafetyException</code> is
 * thrown which case the server is disabled and remains in <code>Mode.NON_TRANSIENT_ERROR</code>.
 *
 * @author lyubent
 */
public class BootstrappingSafetyException extends RuntimeException {

    private String mode;
    private String mostUtilizedDataDir;
    private Double utilization;
    private Double threshold;

    public BootstrappingSafetyException(String mode, String mostUtilizedDataDir, Double utilization, Double threshold)
    {
        this.mode = mode;
        this.mostUtilizedDataDir = mostUtilizedDataDir;
        this.utilization = utilization;
        this.threshold = threshold;
    }

    public String getLocalizedMessage()
    {
        return getMessage();
    }

    public String getMessage()
    {
        String msg = "Preventing node from continuing after failed bootstrap as most utilized data_file_dir (%s) " +
                     "is too full (%f) and exceeds palantir_cassandra.bootstrap_disk_usage_threshold_percentage (%f). " +
                     "Node will stay in %s mode.";
        return String.format(msg, mostUtilizedDataDir, utilization, threshold, mode);
    }
}
