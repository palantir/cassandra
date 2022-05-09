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

package org.apache.cassandra.io.compress;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CQLCompressionTest extends CQLTester
{
    @Test
    public void lz4ParamsTest()
    {

        createTable("create table %s (id int primary key, uh text) with compression = {'sstable_compression':'LZ4Compressor'}");
        ICompressor compressor = CompressionParameters.create(getCurrentColumnFamilyStore().getCompressionParameters()).sstableCompressor;
        assertTrue(compressor instanceof LZ4Compressor);
    }

    @Test(expected = ConfigurationException.class)
    public void lz4BadParamsTest() throws Throwable
    {
        try
        {
            createTable("create table %s (id int primary key, uh text) with compression = {'sstable_compression':'LZ4Compressor', 'lz4_compressor_type':'high', 'lz4_high_compressor_level':113}");
        }
        catch (RuntimeException e)
        {
            throw e.getCause();
        }
    }

    @Test
    public void zstdParamsTest()
    {
        createTable("create table %s (id int primary key, uh text) with compression = {'sstable_compression':'ZstdCompressor', 'compression_level':-22}");
        ICompressor compressor = CompressionParameters.create(getCurrentColumnFamilyStore().getCompressionParameters()).sstableCompressor;
        assertTrue(compressor instanceof ZstdCompressor);
        ZstdCompressor zstdCompressor = (ZstdCompressor) compressor;
        assertEquals(zstdCompressor.getCompressionLevel(), -22);

        createTable("create table %s (id int primary key, uh text) with compression = {'sstable_compression':'ZstdCompressor'}");
        compressor = CompressionParameters.create(getCurrentColumnFamilyStore().getCompressionParameters()).sstableCompressor;
        assertTrue(compressor instanceof ZstdCompressor);
        zstdCompressor = (ZstdCompressor) compressor;
        assertEquals(zstdCompressor.getCompressionLevel(), ZstdCompressor.DEFAULT_COMPRESSION_LEVEL);
    }

    @Test(expected = ConfigurationException.class)
    public void zstdBadParamsTest() throws Throwable
    {
        try
        {
            createTable("create table %s (id int primary key, uh text) with compression = {'sstable_compression':'ZstdCompressor', 'compression_level':'100'}");
        }
        catch (RuntimeException e)
        {
            throw e.getCause();
        }
    }

    private ColumnFamilyStore getCurrentColumnFamilyStore()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
    }
}
