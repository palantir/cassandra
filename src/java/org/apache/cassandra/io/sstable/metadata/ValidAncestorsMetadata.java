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

package org.apache.cassandra.io.sstable.metadata;

import java.io.DataInput;

import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Marker metadata component indicating this SSTable has valid compaction ancestor information.
 */
public class ValidAncestorsMetadata extends MetadataComponent
{
    public static final IMetadataComponentSerializer serializer = new ValidAncestorsMetadataSerializer();

    public MetadataType getType()
    {
        return MetadataType.VALID_ANCESTORS;
    }

    public static class ValidAncestorsMetadataSerializer implements IMetadataComponentSerializer<ValidAncestorsMetadata>
    {
        @Override
        public int serializedSize(ValidAncestorsMetadata component, Version version)
        {
            return 0;
        }

        @Override
        public void serialize(ValidAncestorsMetadata component, Version version, DataOutputPlus out) {}

        @Override
        public ValidAncestorsMetadata deserialize(Version version, DataInput in)
        {
            return new ValidAncestorsMetadata();
        }
    }
}
