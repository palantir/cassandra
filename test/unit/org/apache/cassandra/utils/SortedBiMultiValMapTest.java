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

package org.apache.cassandra.utils;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;

public class SortedBiMultiValMapTest
{
    private static final String KEY_1 = "a";

    private static final String KEY_2 = "b";

    private static final String KEY_3 = "c";

    private static final Integer VALUE_1 = 1;

    private static final SortedBiMultiValMap<String, Integer> MAP = SortedBiMultiValMap.create();

    @BeforeClass
    public static void beforeClass() {
        MAP.put(KEY_1, VALUE_1);
        MAP.put(KEY_2, VALUE_1);
        MAP.put(KEY_3, VALUE_1);
    }

    @Test
    public void create_copyFromExistingMap() {
        SortedBiMultiValMap<String, Integer> copied = SortedBiMultiValMap.create(MAP);
        assertThat(MAP.forwardMap).isEqualTo(copied.forwardMap);
        assertThat(MAP.reverseMap).isEqualTo(copied.reverseMap);
    }

    @Test
    public void create_copyWithCustomCompareOperator() {
        SortedBiMultiValMap<String, Integer> copied = SortedBiMultiValMap.create(MAP, Comparator.reverseOrder(), Comparator.reverseOrder());
        String[] expectedOrder = new String[] { KEY_3, KEY_2, KEY_1 };
        assertThat(new ArrayList<>(copied.forwardMap.keySet())).containsExactly(expectedOrder);
        assertThat(copied.reverseMap.get(VALUE_1)).containsExactly(expectedOrder);
    }
}
