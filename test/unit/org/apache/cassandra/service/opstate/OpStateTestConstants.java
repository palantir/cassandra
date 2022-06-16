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

package org.apache.cassandra.service.opstate;

import org.codehaus.jackson.map.ObjectMapper;

public class OpStateTestConstants
{
    public static final String TEST_STATE_FILE_NAME = "test_node_op_state.json";
    public static final String TEST_TMP_STATE_FILE_NAME = "test_node_op_state.json.tmp";
    public static final String TEST_DIRECTORY_NAME = "test-dir";

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final String KEYSPACE1 = "keyspace1";
    public static final String TABLE1 = "table1";
    public static final KeyspaceTableKey KEYSPACE_TABLE_KEY_1 =
        KeyspaceTableKey.of(OpStateTestConstants.KEYSPACE1, OpStateTestConstants.TABLE1);

    public static final String KEYSPACE2 = "keyspace2";
    public static final String TABLE2 = "table2";
    public static final KeyspaceTableKey KEYSPACE_TABLE_KEY_2 =
    KeyspaceTableKey.of(OpStateTestConstants.KEYSPACE2, OpStateTestConstants.TABLE2);


}
