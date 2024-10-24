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
package org.apache.cassandra.db;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import com.palantir.tracing.CloseableTracer;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.QueryPagers;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.net.MessagingService;

/**
 * It represents a Keyspace.
 */
public class Keyspace
{
    private static final int DEFAULT_PAGE_SIZE = 10000;

    private static final Logger logger = LoggerFactory.getLogger(Keyspace.class);

    private static final String TEST_FAIL_WRITES_KS = System.getProperty("cassandra.test.fail_writes_ks", "");
    private static final boolean TEST_FAIL_WRITES = !TEST_FAIL_WRITES_KS.isEmpty();

    public final KeyspaceMetrics metric;

    // It is possible to call Keyspace.open without a running daemon, so it makes sense to ensure
    // proper directories here as well as in CassandraDaemon.
    static
    {
        if (!Config.isClientMode())
            DatabaseDescriptor.createAllDirectories();
    }

    public final OpOrder writeOrder = new OpOrder();

    /* ColumnFamilyStore per column family */
    private final ConcurrentMap<UUID, ColumnFamilyStore> columnFamilyStores = new ConcurrentHashMap<>();
    private volatile KSMetaData metadata;
    private volatile AbstractReplicationStrategy replicationStrategy;

    public static final Function<String,Keyspace> keyspaceTransformer = new Function<String, Keyspace>()
    {
        public Keyspace apply(String keyspaceName)
        {
            return Keyspace.open(keyspaceName);
        }
    };

    private static volatile boolean initialized = false;
    public static void setInitialized()
    {
        initialized = true;
    }

    public static Keyspace open(String keyspaceName)
    {
        assert initialized || keyspaceName.equals(SystemKeyspace.NAME);
        return open(keyspaceName, Schema.instance, true);
    }

    // to only be used by org.apache.cassandra.tools.Standalone* classes
    public static Keyspace openWithoutSSTables(String keyspaceName)
    {
        return open(keyspaceName, Schema.instance, false);
    }

    private static Keyspace open(String keyspaceName, Schema schema, boolean loadSSTables)
    {
        Keyspace keyspaceInstance = schema.getKeyspaceInstance(keyspaceName);

        if (keyspaceInstance == null)
        {
            // instantiate the Keyspace.  we could use putIfAbsent but it's important to making sure it is only done once
            // per keyspace, so we synchronize and re-check before doing it.
            synchronized (Keyspace.class)
            {
                keyspaceInstance = schema.getKeyspaceInstance(keyspaceName);
                if (keyspaceInstance == null)
                {
                    // open and store the keyspace
                    keyspaceInstance = new Keyspace(keyspaceName, loadSSTables);
                    schema.storeKeyspaceInstance(keyspaceInstance);
                }
            }
        }
        return keyspaceInstance;
    }

    public static Keyspace clear(String keyspaceName)
    {
        return clear(keyspaceName, Schema.instance);
    }

    public static Keyspace clear(String keyspaceName, Schema schema)
    {
        synchronized (Keyspace.class)
        {
            Keyspace t = schema.removeKeyspaceInstance(keyspaceName);
            if (t != null)
            {
                for (ColumnFamilyStore cfs : t.getColumnFamilyStores())
                    t.unloadCf(cfs);
                t.metric.release();
            }
            return t;
        }
    }

    /**
     * Removes every SSTable in the directory from the appropriate Tracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    public static void removeUnreadableSSTables(File directory)
    {
        for (Keyspace keyspace : Keyspace.all())
        {
            for (ColumnFamilyStore baseCfs : keyspace.getColumnFamilyStores())
            {
                for (ColumnFamilyStore cfs : baseCfs.concatWithIndexes())
                    cfs.maybeRemoveUnreadableSSTables(directory);
            }
        }
    }

    public Collection<ColumnFamilyStore> getColumnFamilyStores()
    {
        return Collections.unmodifiableCollection(columnFamilyStores.values());
    }

    public ColumnFamilyStore getColumnFamilyStore(String cfName)
    {
        UUID id = Schema.instance.getId(getName(), cfName);
        if (id == null)
            throw new IllegalArgumentException(String.format("Unknown keyspace/cf pair (%s.%s)", getName(), cfName));
        return getColumnFamilyStore(id);
    }

    public ColumnFamilyStore getColumnFamilyStore(UUID id)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(id);
        if (cfs == null)
            throw new IllegalArgumentException("Unknown CF " + id);
        return cfs;
    }

    /**
     * Take a snapshot of the specific column family, or the entire set of column families
     * if columnFamily is null with a given timestamp
     *
     * @param snapshotName     the tag associated with the name of the snapshot.  This value may not be null
     * @param columnFamilyName the column family to snapshot or all on null
     * @throws IOException if the column family doesn't exist
     */
    public void snapshot(String snapshotName, String columnFamilyName) throws IOException
    {
        snapshot(snapshotName, columnFamilyName, false);
    }

    /**
     * Take a snapshot of the specific column family, or the entire set of column families
     * if columnFamily is null with a given timestamp
     *
     * @param snapshotName     the tag associated with the name of the snapshot.  This value may not be null
     * @param columnFamilyName the column family to snapshot or all on null
     * @param ephemeral If this flag is set to true, the snapshot will be cleaned up during next startup
     * @throws IOException if the column family doesn't exist
     */
    public void snapshot(String snapshotName, String columnFamilyName, boolean ephemeral) throws IOException
    {
        assert snapshotName != null;
        boolean tookSnapShot = false;
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
        {
            if (columnFamilyName == null || cfStore.name.equals(columnFamilyName))
            {
                logger.debug("Attempting snapshot with snapshotName {} of cf {}", snapshotName, columnFamilyName);
                tookSnapShot = true;
                cfStore.snapshot(snapshotName, null, ephemeral);
                logger.debug("Finished snapshot with snapshotName {} of cf {}", snapshotName, columnFamilyName);
            }
        }

        if ((columnFamilyName != null) && !tookSnapShot)
            throw new IOException("Failed taking snapshot. Table " + columnFamilyName + " does not exist.");
    }

    /**
     * @param clientSuppliedName may be null.
     * @return the name of the snapshot
     */
    public static String getTimestampedSnapshotName(String clientSuppliedName)
    {
        String snapshotName = Long.toString(System.currentTimeMillis());
        if (clientSuppliedName != null && !clientSuppliedName.equals(""))
        {
            snapshotName = snapshotName + "-" + clientSuppliedName;
        }
        return snapshotName;
    }

    /**
     * Check whether snapshots already exists for a given name.
     *
     * @param snapshotName the user supplied snapshot name
     * @return true if the snapshot exists
     */
    public boolean snapshotExists(String snapshotName)
    {
        assert snapshotName != null;
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
        {
            if (cfStore.snapshotExists(snapshotName))
                return true;
        }
        return false;
    }

    /**
     * Clear all the snapshots for a given keyspace.
     *
     * @param snapshotName the user supplied snapshot name. It empty or null,
     *                     all the snapshots will be cleaned
     */
    public static void clearSnapshot(String snapshotName, String keyspace)
    {
        List<File> snapshotDirs = Directories.getKSChildDirectories(keyspace);
        Directories.clearSnapshot(snapshotName, snapshotDirs);
    }

    /**
     * @return A list of open SSTableReaders
     */
    public List<SSTableReader> getAllSSTables()
    {
        List<SSTableReader> list = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
            list.addAll(cfStore.getSSTables());
        return list;
    }

    private Keyspace(String keyspaceName, boolean loadSSTables)
    {
        metadata = Schema.instance.getKSMetaData(keyspaceName);
        assert metadata != null : "Unknown keyspace " + keyspaceName;
        createReplicationStrategy(metadata);

        this.metric = new KeyspaceMetrics(this);
        for (CFMetaData cfm : new ArrayList<>(metadata.cfMetaData().values()))
        {
            logger.trace("Initializing {}.{}", getName(), cfm.cfName);
            initCf(cfm, loadSSTables);
        }
    }

    private Keyspace(KSMetaData metadata)
    {
        this.metadata = metadata;
        createReplicationStrategy(metadata);
        this.metric = new KeyspaceMetrics(this);
    }

    public static Keyspace mockKS(KSMetaData metadata)
    {
        return new Keyspace(metadata);
    }

    private void createReplicationStrategy(KSMetaData ksm)
    {
        replicationStrategy = AbstractReplicationStrategy.createReplicationStrategy(ksm.name,
                                                                                    ksm.strategyClass,
                                                                                    StorageService.instance.getTokenMetadata(),
                                                                                    DatabaseDescriptor.getEndpointSnitch(),
                                                                                    ksm.strategyOptions);
    }

    public void setMetadata(KSMetaData ksm)
    {
        this.metadata = ksm;
        createReplicationStrategy(ksm);
    }

    public KSMetaData getMetadata()
    {
        return metadata;
    }

    // best invoked on the compaction mananger.
    public void dropCf(UUID cfId)
    {
        assert columnFamilyStores.containsKey(cfId);
        ColumnFamilyStore cfs = columnFamilyStores.remove(cfId);
        if (cfs == null)
            return;

        cfs.getCompactionStrategy().shutdown();
        CompactionManager.instance.interruptCompactionForCFs(cfs.concatWithIndexes(), true);
        // wait for any outstanding reads/writes that might affect the CFS
        cfs.keyspace.writeOrder.awaitNewBarrier();
        cfs.readOrdering.awaitNewBarrier();

        unloadCf(cfs);
    }

    // disassociate a cfs from this keyspace instance.
    private void unloadCf(ColumnFamilyStore cfs)
    {
        cfs.forceBlockingFlush("Dropping CF");
        cfs.invalidate();
    }

    /**
     * adds a cf to internal structures, ends up creating disk files).
     */
    public void initCf(CFMetaData metadata, boolean loadSSTables)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(metadata.cfId);

        if (cfs == null)
        {
            // CFS being created for the first time, either on server startup or new CF being added.
            StorageService.instance.createCleanupEntryForCfIfNotExists(metadata.ksName, metadata.cfName, Optional.of(Instant.now()));
            // We don't worry about races here; startup is safe, and adding multiple idential CFs
            // simultaneously is a "don't do that" scenario.
            ColumnFamilyStore oldCfs = columnFamilyStores.putIfAbsent(metadata.cfId, ColumnFamilyStore.createColumnFamilyStore(this, metadata, loadSSTables));
            // CFS mbean instantiation will error out before we hit this, but in case that changes...
            if (oldCfs != null)
                throw new IllegalStateException("added multiple mappings for cf id " + metadata.cfId);
        }
        else
        {
            // re-initializing an existing CF.  This will happen if you cleared the schema
            // on this node and it's getting repopulated from the rest of the cluster.
            assert cfs.name.equals(metadata.cfName);
            cfs.metadata.reload();
            cfs.reload("CF initialization");
        }
    }

    public Row getRow(QueryFilter filter)
    {
        ColumnFamilyStore cfStore = getColumnFamilyStore(filter.getColumnFamilyName());
        ColumnFamily columnFamily = cfStore.getColumnFamily(filter);
        Row row = new Row(filter.key, columnFamily);
        cfStore.metric.readBytesRead.mark(Row.serializer.serializedSize(row, MessagingService.current_version));
        return row;
    }

    public void apply(Mutation mutation, boolean writeCommitLog)
    {
        apply(mutation, writeCommitLog, true);
    }

    /**
     * This method appends a row to the global CommitLog, then updates memtables and indexes.
     *
     * @param mutation       the row to write.  Must not be modified after calling apply, since commitlog append
     *                       may happen concurrently, depending on the CL Executor type.
     * @param writeCommitLog false to disable commitlog append entirely
     * @param updateIndexes  false to disable index updates (used by CollationController "defragmenting")
     */
    public void apply(Mutation mutation, boolean writeCommitLog, boolean updateIndexes)
    {
        try (CloseableTracer ignored = CloseableTracer.startSpan("Keyspace#apply"))
        {
            if (TEST_FAIL_WRITES && metadata.name.equals(TEST_FAIL_WRITES_KS))
                throw new RuntimeException("Testing write failures");

            try (OpOrder.Group opGroup = writeOrder.start())
            {
                // write the mutation to the commitlog and memtables
                ReplayPosition replayPosition = null;
                if (writeCommitLog)
                {
                    Tracing.trace("Appending to commitlog");
                    replayPosition = CommitLog.instance.add(mutation);
                }

                DecoratedKey key = StorageService.getPartitioner().decorateKey(mutation.key());
                for (ColumnFamily cf : mutation.getColumnFamilies())
                {
                    ColumnFamilyStore cfs = columnFamilyStores.get(cf.id());
                    if (cfs == null)
                    {
                        logger.error("Attempting to mutate non-existant table {}", cf.id());
                        continue;
                    }

                    Tracing.trace("Adding to {} memtable", cf.metadata().cfName);
                    SecondaryIndexManager.Updater updater = updateIndexes
                                                            ? cfs.indexManager.updaterFor(key, cf, opGroup)
                                                            : SecondaryIndexManager.nullUpdater;
                    cfs.apply(key, cf, updater, opGroup, replayPosition);
                }
            }
        }
    }

    public AbstractReplicationStrategy getReplicationStrategy()
    {
        return replicationStrategy;
    }

    /**
     * @param key row to index
     * @param cfs ColumnFamily to index row in
     * @param idxNames columns to index, in comparator order
     */
    public static void indexRow(DecoratedKey key, ColumnFamilyStore cfs, Set<String> idxNames)
    {
        if (logger.isTraceEnabled())
            logger.trace("Indexing row {} ", cfs.metadata.getKeyValidator().getString(key.getKey()));

        Set<SecondaryIndex> indexes = cfs.indexManager.getIndexesByNames(idxNames);

        Iterator<ColumnFamily> pager = QueryPagers.pageRowLocally(cfs, key.getKey(), DEFAULT_PAGE_SIZE);
        while (pager.hasNext())
        {
            try (OpOrder.Group opGroup = cfs.keyspace.writeOrder.start()) {
                ColumnFamily cf = pager.next();
                ColumnFamily cf2 = cf.cloneMeShallow();
                for (Cell cell : cf)
                {
                    if (cfs.indexManager.indexes(cell.name(), indexes))
                        cf2.addColumn(cell);
                }
                cfs.indexManager.indexRow(key.getKey(), cf2, opGroup);
            }
        }
    }

    public List<Future<?>> flush()
    {
        List<Future<?>> futures = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfs : columnFamilyStores.values())
            futures.add(cfs.forceFlush("Blocking for writes upon recovery"));
        return futures;
    }

    public Iterable<ColumnFamilyStore> getValidColumnFamilies(boolean allowIndexes, boolean autoAddIndexes, String... cfNames) throws IOException
    {
        Set<ColumnFamilyStore> valid = new HashSet<>();

        if (cfNames.length == 0)
        {
            // all stores are interesting
            for (ColumnFamilyStore cfStore : getColumnFamilyStores())
            {
                valid.add(cfStore);
                if (autoAddIndexes)
                {
                    for (SecondaryIndex si : cfStore.indexManager.getIndexes())
                    {
                        if (si.getIndexCfs() != null) {
                            logger.info("adding secondary index {} to operation", si.getIndexName());
                            valid.add(si.getIndexCfs());
                        }
                    }

                }
            }
            return valid;
        }
        // filter out interesting stores
        for (String cfName : cfNames)
        {
            //if the CF name is an index, just flush the CF that owns the index
            String baseCfName = cfName;
            String idxName = null;
            if (cfName.contains(".")) // secondary index
            {
                if(!allowIndexes)
                {
                    logger.warn("Operation not allowed on secondary Index table ({})", cfName);
                    continue;
                }

                String[] parts = cfName.split("\\.", 2);
                baseCfName = parts[0];
                idxName = parts[1];
            }

            ColumnFamilyStore cfStore = getColumnFamilyStore(baseCfName);
            if (idxName != null)
            {
                Collection< SecondaryIndex > indexes = cfStore.indexManager.getIndexesByNames(new HashSet<>(Arrays.asList(cfName)));
                if (indexes.isEmpty())
                    throw new IllegalArgumentException(String.format("Invalid index specified: %s/%s.", baseCfName, idxName));
                else
                    valid.add(Iterables.get(indexes, 0).getIndexCfs());
            }
            else
            {
                valid.add(cfStore);
                if(autoAddIndexes)
                {
                    for(SecondaryIndex si : cfStore.indexManager.getIndexes())
                    {
                        if (si.getIndexCfs() != null) {
                            logger.info("adding secondary index {} to operation", si.getIndexName());
                            valid.add(si.getIndexCfs());
                        }
                    }
                }
            }
        }
        return valid;
    }

    public static Iterable<Keyspace> all()
    {
        return Iterables.transform(Schema.instance.getKeyspaces(), keyspaceTransformer);
    }

    public static Iterable<Keyspace> nonSystem()
    {
        return Iterables.transform(Schema.instance.getNonSystemKeyspaces(), keyspaceTransformer);
    }

    public static Iterable<Keyspace> system()
    {
        return Iterables.transform(Collections.singleton(SystemKeyspace.NAME), keyspaceTransformer);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(name='" + getName() + "')";
    }

    public String getName()
    {
        return metadata.name;
    }
}
