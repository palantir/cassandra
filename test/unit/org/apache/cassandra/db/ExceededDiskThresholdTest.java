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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionsTest;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.io.ExceededDiskThresholdException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.SimpleStrategy;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;

@RunWith(OrderedJUnit4ClassRunner.class)
public class ExceededDiskThresholdTest
{

    private static final String KEYSPACE_1 = "ExceededDiskThresholdTest1";
    private static final String KEYSPACE_2 = "ExceededDiskThresholdTest2";
    private static final String CF_STANDARD = "CF_STANDARD";
    private static File tempDataDir;

    @BeforeClass
    public static void beforeClass() throws IOException
    {
        tempDataDir = File.createTempFile("cassandra", "unittest");
        tempDataDir.delete(); // hack to create a temp dir
        tempDataDir.mkdir();
        DirectoriesTest.overrideDataDirectoriesForTest(tempDataDir.getPath());
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE_1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE_1, CF_STANDARD));
        SchemaLoader.createKeyspace(KEYSPACE_2,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE_2, CF_STANDARD));
    }


    @Before
    public void before()
    {
        DirectoriesTest.overrideDataDirectoriesForTest(tempDataDir.getPath());
    }

    @After
    public void after() throws NoSuchFieldException, IllegalAccessException
    {
        resetPostFlushExecutor();
    }

    @AfterClass
    public static void afterClass() throws IOException
    {
        Directories.resetDataDirectoriesAfterTest();
        FileUtils.deleteRecursive(tempDataDir);
        SchemaLoader.cleanupAndLeaveDirs();
    }

    @Test
    public void testExceededDiskThrowsOnFlush()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE_1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD);
        store.clearUnsafe();
        store.metadata.gcGraceSeconds(1);

        store.disableAutoCompaction();
        CompactionsTest.populate(KEYSPACE_1, CF_STANDARD, 0, 1, 3);

        // 412 bytes to be flushed. Mock so there are 412 available bytes, but we're still above the allowed threshold
        long total = 100000L;
        long available = 500L;
        doReturn(available).when(Directories.dataDirectories[0]).getAvailableSpace();
        doReturn(total).when(Directories.dataDirectories[0]).getTotalSpace();
        assertThatThrownBy(store::forceBlockingFlush).hasRootCauseInstanceOf(ExceededDiskThresholdException.class);
    }

    @Test
    public void testExceededDiskThrowsOnCompaction()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE_2);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD);
        store.clearUnsafe();
        store.metadata.gcGraceSeconds(1);
        store.setCompactionStrategyClass(SizeTieredCompactionStrategy.class.getCanonicalName());

        store.disableAutoCompaction();
        CompactionsTest.populate(KEYSPACE_2, CF_STANDARD, 0, 1, 3);
        store.forceBlockingFlush();

        // 498 bytes to be written
        long total = 100000L;
        long available = 500L;
        for (Directories.DataDirectory dir : Directories.dataDirectories)
        {
            doReturn(total, available).when(dir).getAvailableSpace();
            doReturn(total).when(dir).getTotalSpace();
        }
        store.enableAutoCompaction();

        assertThatThrownBy(() -> CompactionManager.instance.performMaximal(store, false))
                .hasRootCauseInstanceOf(ExceededDiskThresholdException.class);
    }

    private void resetPostFlushExecutor() throws NoSuchFieldException, IllegalAccessException
    {
        // Without this only 1 test can throw an error during flush since the others will be rejected until restart
        ColumnFamilyStore.previousFlushFailure = null;
        JMXEnabledThreadPoolExecutor executor = (JMXEnabledThreadPoolExecutor) ColumnFamilyStore.postFlushExecutor;
        executor.shutdown();
        Field field = ColumnFamilyStore.class.getDeclaredField("postFlushExecutor");
        field.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(null, new JMXEnabledThreadPoolExecutor(1,
                                                         StageManager.KEEPALIVE,
                                                         TimeUnit.SECONDS,
                                                         new LinkedBlockingQueue<Runnable>(),
                                                         new NamedThreadFactory("MemtablePostFlush"),
                                                         "internal"));
    }

}
