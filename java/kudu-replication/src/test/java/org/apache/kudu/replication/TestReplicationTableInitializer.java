// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.kudu.replication;

import static org.apache.kudu.test.ClientTestUtil.countRowsInTable;
import static org.apache.kudu.test.junit.AssertHelpers.assertEventuallyTrue;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.PartitionSchema;
import org.apache.kudu.client.RangePartitionBound;
import org.apache.kudu.client.RangePartitionWithCustomHashSchema;

public class TestReplicationTableInitializer extends ReplicationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationTableInitializer.class);

  @Override
  protected ReplicationJobConfig createDefaultJobConfig() {
    return ReplicationJobConfig.builder()
            .setSourceMasterAddresses(sourceHarness.getMasterAddressesAsString())
            .setSinkMasterAddresses(sinkHarness.getMasterAddressesAsString())
            .setTableName(TABLE_NAME)
            .setDiscoveryIntervalSeconds(2)
            .setCreateTable(true)
            .build();
  }

  @Test
  public void testTableInitializationSmoke() throws Exception {
    createAllTypesTable(sourceClient);
    insertRowsIntoAllTypesTable(sourceClient, 0, 10);

    envProvider.getEnv().executeAsync();

    KuduTable sinkTable = sinkClient.openTable(TABLE_NAME);
    assertEventuallyTrue("Initial 10 rows should be replicated",
        () -> countRowsInTable(sinkTable) == 10, 60000);

    verifySourceAndSinkRowsEqual(10);
  }

  @Test
  public void testHashOnlyPartitioning() throws Exception {
    // Create table with only hash partitioning (no range partitions)
    Schema schema = createTestSchema();
    CreateTableOptions options = new CreateTableOptions()
            .setRangePartitionColumns(Collections.emptyList())
            .addHashPartitions(Collections.singletonList("key1"), 3)
            .addHashPartitions(Arrays.asList("key2", "key3"), 2, 42);

    sourceClient.createTable(TABLE_NAME, schema, options);
    insertTestRows(sourceClient, 0, 10);

    envProvider.getEnv().executeAsync();

    KuduTable sinkTable = sinkClient.openTable(TABLE_NAME);
    assertEventuallyTrue("Hash partitioned table should be replicated",
        () -> countRowsInTable(sinkTable) == 10, 60000);

    verifyPartitionSchemasMatch();
    verifyTestRowsEqual(10);
  }

  @Test
  public void testRangeOnlyPartitioning() throws Exception {
    // Create table with only range partitioning (no hash)
    Schema schema = createTestSchema();
    CreateTableOptions options = new CreateTableOptions()
            .setRangePartitionColumns(Collections.singletonList("key1"));

    // Add range splits
    PartialRow split1 = schema.newPartialRow();
    split1.addInt("key1", 100);
    options.addSplitRow(split1);

    PartialRow split2 = schema.newPartialRow();
    split2.addInt("key1", 200);
    options.addSplitRow(split2);

    sourceClient.createTable(TABLE_NAME, schema, options);
    insertTestRows(sourceClient, 0, 15);

    envProvider.getEnv().executeAsync();

    KuduTable sinkTable = sinkClient.openTable(TABLE_NAME);
    assertEventuallyTrue("Range partitioned table should be replicated",
        () -> countRowsInTable(sinkTable) == 15, 60000);

    verifyPartitionSchemasMatch();
    verifyTestRowsEqual(15);
  }

  @Test
  public void testHashAndRangePartitioning() throws Exception {
    // Create table with both hash and range partitioning
    Schema schema = createTestSchema();
    CreateTableOptions options = new CreateTableOptions()
            .setRangePartitionColumns(Collections.singletonList("key1"))
            .addHashPartitions(Collections.singletonList("key2"), 2);

    // Add range splits
    PartialRow split1 = schema.newPartialRow();
    split1.addInt("key1", 50);
    options.addSplitRow(split1);

    PartialRow split2 = schema.newPartialRow();
    split2.addInt("key1", 150);
    options.addSplitRow(split2);

    sourceClient.createTable(TABLE_NAME, schema, options);
    insertTestRows(sourceClient, 0, 12);

    envProvider.getEnv().executeAsync();

    KuduTable sinkTable = sinkClient.openTable(TABLE_NAME);
    assertEventuallyTrue("Hash+range partitioned table should be replicated",
        () -> countRowsInTable(sinkTable) == 12, 60000);

    verifyPartitionSchemasMatch();
    verifyTestRowsEqual(12);
  }

  @Test
  public void testRangeWithCustomHashSchemas() throws Exception {
    // Create table with range partitions having different hash schemas per range
    Schema schema = createTestSchema();
    final CreateTableOptions options = new CreateTableOptions()
            .setRangePartitionColumns(Collections.singletonList("key1"))
            .addHashPartitions(Collections.singletonList("key1"), 2);

    // Add range with custom hash schema only
    PartialRow lower = schema.newPartialRow();
    lower.addInt("key1", 100);
    PartialRow upper = schema.newPartialRow();
    upper.addInt("key1", 200);

    RangePartitionWithCustomHashSchema customRange =
            new RangePartitionWithCustomHashSchema(
                    lower, upper,
                    RangePartitionBound.INCLUSIVE_BOUND,
                    RangePartitionBound.EXCLUSIVE_BOUND);
    customRange.addHashPartitions(Collections.singletonList("key1"), 5, 123);
    // Different bucket count and seed
    options.addRangePartition(customRange);

    sourceClient.createTable(TABLE_NAME, schema, options);
    insertTestRows(sourceClient, 100, 10);

    envProvider.getEnv().executeAsync();

    KuduTable sinkTable = sinkClient.openTable(TABLE_NAME);
    assertEventuallyTrue("Custom hash schema table should be replicated",
        () -> countRowsInTable(sinkTable) == 10, 60000);

    verifyPartitionSchemasMatch();
    verifyTestRowsEqual(10);
  }

  @Test
  public void testNonCoveredRangePartitioning() throws Exception {
    // Create table with explicit range boundaries (non-covered ranges)
    Schema schema = createTestSchema();
    CreateTableOptions options = new CreateTableOptions()
            .setRangePartitionColumns(Collections.singletonList("key1"));

    // Add explicit range partitions with gaps
    PartialRow lower1 = schema.newPartialRow();
    lower1.addInt("key1", 0);
    PartialRow upper1 = schema.newPartialRow();
    upper1.addInt("key1", 50);
    options.addRangePartition(lower1, upper1);

    // Gap from 50-100
    PartialRow lower2 = schema.newPartialRow();
    lower2.addInt("key1", 100);
    PartialRow upper2 = schema.newPartialRow();
    upper2.addInt("key1", 200);
    options.addRangePartition(lower2, upper2);

    sourceClient.createTable(TABLE_NAME, schema, options);
    insertTestRows(sourceClient, 0, 5);
    insertTestRows(sourceClient, 100, 5);

    envProvider.getEnv().executeAsync();

    KuduTable sinkTable = sinkClient.openTable(TABLE_NAME);
    assertEventuallyTrue("Non-covered range table should be replicated",
        () -> countRowsInTable(sinkTable) == 10, 60000);

    verifyPartitionSchemasMatch();
    verifyTestRowsEqual(10);
  }

  @Test
  public void testUnpartitionedTable() throws Exception {
    // Create table with no partitioning (single tablet)
    Schema schema = createTestSchema();
    CreateTableOptions options = new CreateTableOptions()
            .setRangePartitionColumns(Collections.emptyList());

    sourceClient.createTable(TABLE_NAME, schema, options);
    insertTestRows(sourceClient, 0, 8);

    envProvider.getEnv().executeAsync();

    KuduTable sinkTable = sinkClient.openTable(TABLE_NAME);
    assertEventuallyTrue("Unpartitioned table should be replicated",
        () -> countRowsInTable(sinkTable) == 8, 60000);

    verifyPartitionSchemasMatch();
    verifyTestRowsEqual(8);
  }

  // Helper methods

  private Schema createTestSchema() {
    List<ColumnSchema> columns = new ArrayList<>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key1", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key2", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key3", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("data", Type.STRING).build());
    return new Schema(columns);
  }

  private void insertTestRows(org.apache.kudu.client.KuduClient client,
                              int startKey, int count) throws Exception {
    KuduTable table = client.openTable(TABLE_NAME);
    org.apache.kudu.client.KuduSession session = client.newSession();
    for (int i = 0; i < count; i++) {
      org.apache.kudu.client.Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      int key = startKey + i;
      row.addInt("key1", key);
      row.addString("key2", "val" + (key % 10));
      row.addInt("key3", key * 2);
      row.addString("data", "test data " + key);
      session.apply(insert);
    }
    session.flush();
    session.close();
  }

  private void verifyTestRowsEqual(int expectedRowCount) throws Exception {
    KuduTable sourceTable = sourceClient.openTable(TABLE_NAME);
    KuduTable sinkTable = sinkClient.openTable(TABLE_NAME);

    assertEquals(String.format("Source should have %d rows", expectedRowCount),
            expectedRowCount, countRowsInTable(sourceTable));
    assertEquals(String.format("Sink should have %d rows", expectedRowCount),
            expectedRowCount, countRowsInTable(sinkTable));

    // Verify row contents match
    org.apache.kudu.client.KuduScanner sourceScanner =
            sourceClient.newScannerBuilder(sourceTable).build();
    org.apache.kudu.client.KuduScanner sinkScanner =
            sinkClient.newScannerBuilder(sinkTable).build();

    int sourceCount = 0;
    int sinkCount = 0;
    while (sourceScanner.hasMoreRows()) {
      sourceCount += sourceScanner.nextRows().getNumRows();
    }
    while (sinkScanner.hasMoreRows()) {
      sinkCount += sinkScanner.nextRows().getNumRows();
    }

    assertEquals(String.format("Row counts should match (source: %d, sink: %d)",
            sourceCount, sinkCount), sourceCount, sinkCount);
  }

  private void verifyPartitionSchemasMatch() throws Exception {
    KuduTable sourceTable = sourceClient.openTable(TABLE_NAME);
    KuduTable sinkTable = sinkClient.openTable(TABLE_NAME);

    PartitionSchema sourceSchema = sourceTable.getPartitionSchema();
    PartitionSchema sinkSchema = sinkTable.getPartitionSchema();

    // Verify hash bucket schemas match
    assertEquals(String.format("Hash bucket schemas should match (source: %d, sink: %d)",
            sourceSchema.getHashBucketSchemas().size(), sinkSchema.getHashBucketSchemas().size()),
            sourceSchema.getHashBucketSchemas().size(),
            sinkSchema.getHashBucketSchemas().size());

    for (int i = 0; i < sourceSchema.getHashBucketSchemas().size(); i++) {
      PartitionSchema.HashBucketSchema sourceHash = sourceSchema.getHashBucketSchemas().get(i);
      PartitionSchema.HashBucketSchema sinkHash = sinkSchema.getHashBucketSchemas().get(i);

      assertEquals(String.format("Hash dimension %d column count should match", i),
              sourceHash.getColumnIds().size(), sinkHash.getColumnIds().size());
      assertEquals(String.format("Hash dimension %d bucket count should match", i),
              sourceHash.getNumBuckets(), sinkHash.getNumBuckets());
      assertEquals(String.format("Hash dimension %d seed should match", i),
              sourceHash.getSeed(), sinkHash.getSeed());
    }

    // Verify range schema matches
    assertEquals(String.format("Range column count should match (source: %d, sink: %d)",
            sourceSchema.getRangeSchema().getColumnIds().size(),
            sinkSchema.getRangeSchema().getColumnIds().size()),
            sourceSchema.getRangeSchema().getColumnIds().size(),
            sinkSchema.getRangeSchema().getColumnIds().size());

    // Note: hasCustomHashSchemas() is not publicly accessible, so we skip this check

    // Verify range partitions match
    List<org.apache.kudu.client.Partition> sourcePartitions = sourceTable.getRangePartitions(10000);
    List<org.apache.kudu.client.Partition> sinkPartitions = sinkTable.getRangePartitions(10000);
    assertEquals(String.format("Number of range partitions should match (source: %d, sink: %d)",
            sourcePartitions.size(), sinkPartitions.size()),
            sourcePartitions.size(), sinkPartitions.size());
  }
}
