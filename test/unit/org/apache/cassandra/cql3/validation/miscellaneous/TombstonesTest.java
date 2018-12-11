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
package org.apache.cassandra.cql3.validation.miscellaneous;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.palantir.cassandra.db.RowCountOverwhelmingException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;

/**
 * Test that TombstoneOverwhelmingException gets thrown when it should be and doesn't when it shouldn't be.
 */
public class TombstonesTest extends CQLTester
{
    static final int ORIGINAL_TOMBSTONE_FAILURE_THRESHOLD = DatabaseDescriptor.getTombstoneFailureThreshold();
    static final int TOMBSTONE_FAILURE_THRESHOLD = 100;

    static final int ORIGINAL_TOMBSTONE_WARN_THRESHOLD = DatabaseDescriptor.getTombstoneWarnThreshold();
    static final int TOMBSTONE_WARN_THRESHOLD = 50;

    static final int ORIGINAL_ROWCOUNT_FAILURE_THRESHOLD = DatabaseDescriptor.getRowCountFailureThreshold();
    static final int ROWCOUNT_FAILURE_THRESHOLD = 100;

    static final int ORIGINAL_ROWCOUNT_WARN_THRESHOLD = DatabaseDescriptor.getRowCountWarnThreshold();
    static final int ROWCOUNT_WARN_THRESHOLD = 50;

    @BeforeClass
    public static void setUp() throws Throwable
    {
        DatabaseDescriptor.setTombstoneFailureThreshold(TOMBSTONE_FAILURE_THRESHOLD);
        DatabaseDescriptor.setTombstoneWarnThreshold(TOMBSTONE_WARN_THRESHOLD);
        DatabaseDescriptor.setRowCountFailureThreshold(ROWCOUNT_FAILURE_THRESHOLD);
        DatabaseDescriptor.setRowCountWarnThreshold(ROWCOUNT_WARN_THRESHOLD);
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setTombstoneFailureThreshold(ORIGINAL_TOMBSTONE_FAILURE_THRESHOLD);
        DatabaseDescriptor.setTombstoneWarnThreshold(ORIGINAL_TOMBSTONE_WARN_THRESHOLD);
        DatabaseDescriptor.setRowCountFailureThreshold(ORIGINAL_ROWCOUNT_FAILURE_THRESHOLD);
        DatabaseDescriptor.setRowCountWarnThreshold(ORIGINAL_ROWCOUNT_WARN_THRESHOLD);
    }

    @Test
    public void testBelowWarnThresholdSelect() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.tombstoneFailures.getCount();
        long oldWarnings = cfs.metric.tombstoneWarnings.getCount();

        // insert exactly the amount of tombstones that shouldn't trigger an exception
        for (int i = 0; i < TOMBSTONE_WARN_THRESHOLD; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', null);");

        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
        }
        catch (Throwable e)
        {
            fail("SELECT with tombstones below the threshold should not have failed, but has: " + e);
        }

        assertEquals(oldFailures, cfs.metric.tombstoneFailures.getCount());
        assertEquals(oldWarnings, cfs.metric.tombstoneWarnings.getCount());
    }

    @Test
    public void testBeyondWarnThresholdBelowFailureThresholdSelect() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a,b));");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.tombstoneFailures.getCount();
        long oldWarnings = cfs.metric.tombstoneWarnings.getCount();

        // insert the number of tombstones that *SHOULD* trigger an Warning
        for (int i = 0; i < TOMBSTONE_WARN_THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c ) VALUES ('key', 'cc" + i + "',  null);");
        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
            assertEquals(oldWarnings + 1, cfs.metric.tombstoneWarnings.getCount());
            assertEquals(oldFailures, cfs.metric.tombstoneFailures.getCount());
        }
        catch (Throwable e)
        {
            fail("SELECT with tombstones below the failure threshold and above warning threashhold should not have failed, but has: " + e);
        }
    }

    @Test
    public void testBeyondThresholdSelect() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.tombstoneFailures.getCount();
        long oldWarnings = cfs.metric.tombstoneWarnings.getCount();

        // insert exactly the amount of tombstones that *SHOULD* trigger an exception
        for (int i = 0; i < TOMBSTONE_FAILURE_THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', null);");

        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
            fail("SELECT with tombstones beyond the threshold should have failed, but hasn't");
        }
        catch (Throwable e)
        {
            String error = "Expected exception instanceof TombstoneOverwhelmingException instead got "
                          + System.lineSeparator()
                          + Throwables.getStackTraceAsString(e);
            assertTrue(error, e instanceof TombstoneOverwhelmingException);
        }

        assertEquals(oldFailures + 1, cfs.metric.tombstoneFailures.getCount());
        assertEquals(oldWarnings, cfs.metric.tombstoneWarnings.getCount());
    }

    @Test
    public void testAllShadowedSelect() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.tombstoneFailures.getCount();
        long oldWarnings = cfs.metric.tombstoneWarnings.getCount();

        // insert exactly the amount of tombstones that *SHOULD* normally trigger an exception
        for (int i = 0; i < TOMBSTONE_FAILURE_THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', null);");

        // delete all with a partition level tombstone
        execute("DELETE FROM %s WHERE a = 'key'");

        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
        }
        catch (Throwable e)
        {
            fail("SELECT with tombstones shadowed by a partition tombstone should not have failed, but has: " + e);
        }

        // we don't check the failure threshold if the tombstone is shadowed; see SliceQueryFilter line 280
        assertEquals(oldFailures, cfs.metric.tombstoneFailures.getCount());
        assertEquals(oldWarnings+1, cfs.metric.tombstoneWarnings.getCount());
    }

    @Test
    public void testLiveShadowedCellsSelect() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.tombstoneFailures.getCount();
        long oldWarnings = cfs.metric.tombstoneWarnings.getCount();

        for (int i = 0; i < TOMBSTONE_FAILURE_THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', 'column');");

        // delete all with a partition level tombstone
        execute("DELETE FROM %s WHERE a = 'key'");

        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
        }
        catch (Throwable e)
        {
            fail("SELECT with regular cells shadowed by a partition tombstone should not have failed, but has: " + e);
        }

        assertEquals(oldFailures, cfs.metric.tombstoneFailures.getCount());
        assertEquals(oldWarnings, cfs.metric.tombstoneWarnings.getCount());
    }

    @Test
    public void testExpiredTombstones() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b))"
                + " WITH gc_grace_seconds = 1;");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.tombstoneFailures.getCount();
        long oldWarnings = cfs.metric.tombstoneWarnings.getCount();

        for (int i = 0; i < TOMBSTONE_FAILURE_THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', null);");

        // not yet past gc grace - must throw a TOE
        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
            fail("SELECT with tombstones beyond the threshold should have failed, but hasn't");
        }
        catch (Throwable e)
        {
            assertTrue(e instanceof TombstoneOverwhelmingException);
        }

        assertEquals(++oldFailures, cfs.metric.tombstoneFailures.getCount());
        assertEquals(oldWarnings, cfs.metric.tombstoneWarnings.getCount());

        // sleep past gc grace
        TimeUnit.SECONDS.sleep(2);

        // past gc grace - must not throw a TOE now
        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
        }
        catch (Throwable e)
        {
            fail("SELECT with expired tombstones beyond the threshold should not have failed, but has: " + e);
        }

        assertEquals(oldFailures, cfs.metric.tombstoneFailures.getCount());
        assertEquals(++oldWarnings, cfs.metric.tombstoneWarnings.getCount());
    }

    @Test
    public void testBelowRowCountWarnThresholdSelect() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.rowCountFailures.getCount();
        long oldWarnings = cfs.metric.rowCountWarnings.getCount();

        // insert exactly the amount of rows that shouldn't trigger an exception
        for (int i = 0; i < TOMBSTONE_WARN_THRESHOLD; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key" + i + "', 'column', 'val');");

        try
        {
            execute("SELECT * FROM %s;");
        }
        catch (Throwable e)
        {
            fail("SELECT with row count below the threshold should not have failed, but has: " + e);
        }

        assertEquals(oldFailures, cfs.metric.rowCountFailures.getCount());
        assertEquals(oldWarnings, cfs.metric.rowCountWarnings.getCount());
    }

    @Test
    public void testBeyondWarnBelowFailureRowCountThresholdSelect() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.rowCountFailures.getCount();
        long oldWarnings = cfs.metric.rowCountWarnings.getCount();

        // insert exactly the amount of rows that shouldn't trigger an exception
        for (int i = 0; i < TOMBSTONE_FAILURE_THRESHOLD; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key" + i + "', 'column', 'val');");

        try
        {
            execute("SELECT * FROM %s;");
        }
        catch (Throwable e)
        {
            fail("SELECT with row count below the threshold should not have failed, but has: " + e);
        }

        assertEquals(oldFailures, cfs.metric.rowCountFailures.getCount());
        assertEquals(oldWarnings+1, cfs.metric.rowCountWarnings.getCount());
    }

    @Test
    public void testBeyondRowCountThresholdSelect() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.rowCountFailures.getCount();
        long oldWarnings = cfs.metric.rowCountWarnings.getCount();

        // insert exactly the amount of rows that *SHOULD* trigger an exception
        for (int i = 0; i < TOMBSTONE_FAILURE_THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key" + i + "', 'column', 'val');");

        try
        {
            execute("SELECT * FROM %s;");
            fail("SELECT with row count beyond the threshold should have failed, but hasn't");
        }
        catch (Throwable e)
        {
            String error = "Expected exception instanceof RowCountOverwhelmingException instead got "
                           + System.lineSeparator()
                           + Throwables.getStackTraceAsString(e);
            assertTrue(error, e instanceof RowCountOverwhelmingException);
        }

        assertEquals(oldFailures+1, cfs.metric.rowCountFailures.getCount());
        assertEquals(oldWarnings, cfs.metric.rowCountWarnings.getCount());
    }

    @Test
    public void testBeyondRowsThresholdSelectWithAllTombstones() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.rowCountFailures.getCount();
        long oldWarnings = cfs.metric.rowCountWarnings.getCount();

        // insert exactly the amount of rows that *SHOULD* normally trigger an exception
        for (int i = 0; i < TOMBSTONE_FAILURE_THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key" + i + "', 'column', null);");

        try
        {
            execute("SELECT * FROM %s;");
            fail("SELECT with row count beyond the threshold should have failed, but hasn't");
        }
        catch (Throwable e)
        {
            String error = "Expected exception instanceof RowCountOverwhelmingException instead got "
                           + System.lineSeparator()
                           + Throwables.getStackTraceAsString(e);
            assertTrue(error, e instanceof RowCountOverwhelmingException);
        }

        assertEquals(oldFailures+1, cfs.metric.rowCountFailures.getCount());
        assertEquals(oldWarnings, cfs.metric.rowCountWarnings.getCount());
    }

    @Test
    public void testBeyondRowsThresholdSelectWithAllRowLevelTombstones() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.rowCountFailures.getCount();
        long oldWarnings = cfs.metric.rowCountWarnings.getCount();

        // insert exactly the amount of rows that *SHOULD* normally trigger an exception
        for (int i = 0; i < TOMBSTONE_FAILURE_THRESHOLD + 1; i++)
        {
            execute("INSERT INTO %s (a, b, c) VALUES ('key" + i + "', 'column', 'val');");

            // delete row with a partition level tombstone
            execute("DELETE FROM %s WHERE a = 'key" + i + "'");
        }

        try
        {
            execute("SELECT * FROM %s;");
            fail("SELECT with row count beyond the threshold should have failed, but hasn't");
        }
        catch (Throwable e)
        {
            String error = "Expected exception instanceof RowCountOverwhelmingException instead got "
                           + System.lineSeparator()
                           + Throwables.getStackTraceAsString(e);
            assertTrue(error, e instanceof RowCountOverwhelmingException);
        }

        assertEquals(oldFailures+1, cfs.metric.rowCountFailures.getCount());
        assertEquals(oldWarnings, cfs.metric.rowCountWarnings.getCount());
    }
}
