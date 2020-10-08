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

package com.palantir.cassandra.thrift;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.CfSplit;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyPredicate;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.MultiSliceRequest;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

public class DummyCassandra implements Cassandra.Iface
{
    public void login(AuthenticationRequest auth_request) throws AuthenticationException, AuthorizationException, TException
    {

    }

    public void set_keyspace(String keyspace) throws InvalidRequestException, TException
    {

    }

    public ColumnOrSuperColumn get(ByteBuffer key, ColumnPath column_path, ConsistencyLevel consistency_level) throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException
    {
        return null;
    }

    public List<ColumnOrSuperColumn> get_slice(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return null;
    }

    public int get_count(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return 0;
    }

    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return null;
    }

    public Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> multiget_multislice(List<KeyPredicate> request, ColumnParent column_parent, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return null;
    }

    public Map<ByteBuffer, Integer> multiget_count(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return null;
    }

    public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return null;
    }

    public List<KeySlice> get_paged_slice(String column_family, KeyRange range, ByteBuffer start_column, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return null;
    }

    public List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return null;
    }

    public void insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {

    }

    public void add(ByteBuffer key, ColumnParent column_parent, CounterColumn column, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {

    }

    public CASResult cas(ByteBuffer key, String column_family, List<Column> expected, List<Column> updates, ConsistencyLevel serial_consistency_level, ConsistencyLevel commit_consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return null;
    }

    public CASResult put_unless_exists(ByteBuffer key, String column_family, List<Column> updates, ConsistencyLevel serial_consistency_level, ConsistencyLevel commit_consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return null;
    }

    public void remove(ByteBuffer key, ColumnPath column_path, long timestamp, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {

    }

    public void remove_counter(ByteBuffer key, ColumnPath path, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {

    }

    public void batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {

    }

    public void atomic_batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {

    }

    public void truncate(String cfname) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {

    }

    public List<ColumnOrSuperColumn> get_multi_slice(MultiSliceRequest request) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        return null;
    }

    public Map<String, List<String>> describe_schema_versions() throws InvalidRequestException, TException
    {
        return null;
    }

    public List<KsDef> describe_keyspaces() throws InvalidRequestException, TException
    {
        return null;
    }

    public String describe_cluster_name() throws TException
    {
        return null;
    }

    public String describe_version() throws TException
    {
        return null;
    }

    public List<TokenRange> describe_ring(String keyspace) throws InvalidRequestException, TException
    {
        return null;
    }

    public List<TokenRange> describe_local_ring(String keyspace) throws InvalidRequestException, TException
    {
        return null;
    }

    public Map<String, String> describe_token_map() throws InvalidRequestException, TException
    {
        return null;
    }

    public String describe_partitioner() throws TException
    {
        return null;
    }

    public String describe_snitch() throws TException
    {
        return null;
    }

    public KsDef describe_keyspace(String keyspace) throws NotFoundException, InvalidRequestException, TException
    {
        return null;
    }

    public List<String> describe_splits(String cfName, String start_token, String end_token, int keys_per_split) throws InvalidRequestException, TException
    {
        return null;
    }

    public ByteBuffer trace_next_query() throws TException
    {
        return null;
    }

    public List<CfSplit> describe_splits_ex(String cfName, String start_token, String end_token, int keys_per_split) throws InvalidRequestException, TException
    {
        return null;
    }

    public String system_add_column_family(CfDef cf_def) throws InvalidRequestException, SchemaDisagreementException, TException
    {
        return null;
    }

    public String system_drop_column_family(String column_family) throws InvalidRequestException, SchemaDisagreementException, TException
    {
        return null;
    }

    public String system_add_keyspace(KsDef ks_def) throws InvalidRequestException, SchemaDisagreementException, TException
    {
        return null;
    }

    public String system_drop_keyspace(String keyspace) throws InvalidRequestException, SchemaDisagreementException, TException
    {
        return null;
    }

    public String system_update_keyspace(KsDef ks_def) throws InvalidRequestException, SchemaDisagreementException, TException
    {
        return null;
    }

    public String system_update_column_family(CfDef cf_def) throws InvalidRequestException, SchemaDisagreementException, TException
    {
        return null;
    }

    public CqlResult execute_cql_query(ByteBuffer query, Compression compression) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        return null;
    }

    public CqlResult execute_cql3_query(ByteBuffer query, Compression compression, ConsistencyLevel consistency) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        return null;
    }

    public CqlPreparedResult prepare_cql_query(ByteBuffer query, Compression compression) throws InvalidRequestException, TException
    {
        return null;
    }

    public CqlPreparedResult prepare_cql3_query(ByteBuffer query, Compression compression) throws InvalidRequestException, TException
    {
        return null;
    }

    public CqlResult execute_prepared_cql_query(int itemId, List<ByteBuffer> values) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        return null;
    }

    public CqlResult execute_prepared_cql3_query(int itemId, List<ByteBuffer> values, ConsistencyLevel consistency) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        return null;
    }

    public void set_cql_version(String version) throws InvalidRequestException, TException
    {

    }
}
