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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.util.File;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Bulk transfers require all nodes associated with the ranges of the SSTables to be up. This is
 * because Accord requires that the SSTables are on disk in order to run the txn that places them in
 * the active directory.
 * */
public class AccordImportSSTableTest extends TestBaseImpl
{

    private static final String TABLE = "tbl";
    private static final String KEYSPACE_TABLE = String.format("%s.%s", KEYSPACE, TABLE);
    private static final String TABLE_SCHEMA_CQL = String.format(withKeyspace("CREATE TABLE %s." + TABLE + " (k int primary key, v int);"));

    @Test
    public void adaptorExecutionTest() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1)
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");

            cluster.coordinator(1).execute(wrapInTxn("INSERT INTO " + KEYSPACE_TABLE + " (k, v) VALUES (?, ?)"), ConsistencyLevel.SERIAL, 1, 2);
            cluster.coordinator(1).execute(wrapInTxn("SELECT * FROM " + KEYSPACE_TABLE + " WHERE k = 1"), ConsistencyLevel.SERIAL, 1, 2);
        }
    }

    // Chore: Come up with a test case for a multi shard read where
    // we go to two shards and see how the read command flows through for the interop
    @Test
    public void readFromTwoShards() throws Throwable
    {
        String file = Files.createTempDirectory("IMPORT").toString();

        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE + ".tbl (k, v) " + "VALUES (?, ?)");

        try (CQLSSTableWriter writer = builder.build())
        {
            writer.addRow(1, 1);
            writer.addRow(2, 1);
            writer.addRow(3, 1);
        }

        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1)
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");


            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, "tbl");
                Set<String> paths = Set.of(file);
                cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
            });

            Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);


            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).runOnInstance(() -> {
                    ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, "tbl");
                    assertEquals(1, cfs.getLiveSSTables().size());
                });
                /*SimpleQueryResult r1 = cluster.coordinator(1).executeWithResult("BEGIN TRANSACTION \n" +
                                                                                "SELECT * FROM " + KEYSPACE_TABLE + " WHERE k = 1;\n" +
                                                                                "COMMIT TRANSACTION", ConsistencyLevel.ONE);
                assertEquals(1, r1.toObjectArrays().length);*/
            }
        }
    }

    @Test
    public void importIsVisibleTest() throws Throwable
    {
        String file = Files.createTempDirectory("IMPORT").toString();

        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE + ".tbl (k, v) " + "VALUES (?, ?)");

        try (CQLSSTableWriter writer = builder.build())
        {
            writer.addRow(1, 1);
            writer.addRow(2, 1);
            writer.addRow(3, 1);
        }

        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1)
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");


            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, "tbl");
                Set<String> paths = Set.of(file);
                cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
            });

            Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);


            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).runOnInstance(() -> {
                    ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, "tbl");
                    assertEquals(1, cfs.getLiveSSTables().size());
                });
                /*SimpleQueryResult r1 = cluster.coordinator(1).executeWithResult("BEGIN TRANSACTION \n" +
                                                                                "SELECT * FROM " + KEYSPACE_TABLE + " WHERE k = 1;\n" +
                                                                                "COMMIT TRANSACTION", ConsistencyLevel.ONE);
                assertEquals(1, r1.toObjectArrays().length);*/
            }
        }
    }

    @Test
    public void sstablesThatSpanMoreThanOneTableFailsImport() throws Throwable
    {

    }

    @Test
    public void testConcurrentTransactionWithImport()
    {

    }

    @Test
    public void importTxnGoesOnSlowPath()
    {

    }

    @Test
    public void importGetsCleanedUpTest() throws Throwable
    {
        String file = Files.createTempDirectory("IMPORT").toString();

        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE + ".tbl (k, v) " + "VALUES (?, ?)");

        try (CQLSSTableWriter writer = builder.build())
        {
            writer.addRow(1, 1);
            writer.addRow(2, 1);
            writer.addRow(3, 1);
        }

        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1)
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");


            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, "tbl");
                Set<String> paths = Set.of(file);
                cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
            });

            Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
            for (int i = 1; i <= 3; i++)
            {
                SimpleQueryResult r1 = cluster.coordinator(1).executeWithResult("BEGIN TRANSACTION \n" +
                                                                                "SELECT * FROM " + KEYSPACE_TABLE + " WHERE k = 1;\n" +
                                                                                "COMMIT TRANSACTION", ConsistencyLevel.ONE);
                assertEquals(1, r1.toObjectArrays().length);
            }
        }
    }

    @Test
    public void importsOccurAtCorrectNodes() throws Throwable
    {

    }

    /*@Test
    public void importFailsIfExecuteAtEpochDiffers() throws Throwable
    {

    }*/

    /*@Test
    public void importSSTableTest() throws Throwable
    {
        String tableName = "tbl0";
        String qualifiedTableName = KEYSPACE + '.' + tableName;

        String file = Files.createTempDirectory("IMPORT").toString();

        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE + ".tbl0 (k, v) " + "VALUES (?, ?)");

        try (CQLSSTableWriter writer = builder.build())
        {
            writer.addRow(1, 1);
            writer.addRow(2, 1);
            writer.addRow(3, 1);
        }

        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1)
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, "tbl0");
                Set<String> paths = Set.of(file);
                cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
            });

            Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

            for (int i = 1; i <= 3; i++)
            {
                SimpleQueryResult r1 = cluster.coordinator(1).executeWithResult("SELECT * FROM " + qualifiedTableName + " WHERE k = 1", ConsistencyLevel.ONE);
                assertEquals(1, r1.toObjectArrays().length);
            }
        }
    }

    @Test
    public void multiShardImportSSTableTest() throws Throwable
    {
        String tableName = "tbl0";
        String qualifiedTableName = KEYSPACE + '.' + tableName;

        String file = Files.createTempDirectory("IMPORT").toString();

        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE + ".tbl0 (k, v) " + "VALUES (?, ?)");

        try (CQLSSTableWriter writer = builder.build())
        {
            writer.addRow(1, 1);
            writer.addRow(2, 1);
            writer.addRow(3, 1);
        }

        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1)
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, "tbl0");
                Set<String> paths = Set.of(file);
                cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
            });

            Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

            for (int i = 1; i <= 3; i++)
            {
                SimpleQueryResult r1 = cluster.coordinator(1).executeWithResult("SELECT * FROM " + qualifiedTableName + " WHERE k = 1", ConsistencyLevel.ONE);
                assertEquals(1, r1.toObjectArrays().length);
            }
        }
    }

    @Test
    public void changeInEpochImportSSTableTest() throws Throwable
    {

    }

    @Test
    public void failureImportSSTableTest() throws Throwable
    {
        // Chore: Test that fails at the import stage for import sstable
    }*/

    // Test Garbage collection
}
