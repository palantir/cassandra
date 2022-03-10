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

package org.apache.cassandra.service.cleanupstate;

import org.codehaus.jackson.map.ObjectMapper;

public class CleanupStateTestConstants
{
    public static final String TEST_CLEANUP_STATE_FILE_LOCATION =
    System.getProperty("user.dir") + "/test/resources/test_node_cleanup_state.json";

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final String KEYSPACE1 = "keyspace1";
    public static final String TABLE1 = "table1";
    public static final String KEYSPACE1_TABLE1_KEY = KEYSPACE1 + ":" +TABLE1;
    public static final String KEYSPACE2_TABLE2_KEY = "keyspace2:table2";
}
