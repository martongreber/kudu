// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.client;

import static org.apache.kudu.test.ClientTestUtil.createManyStringsSchema;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import com.stumbleupon.async.Deferred;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.KuduTestHarness;

public class TestKuduClientTableAdmin {
  private static final String TABLE_NAME = "TestKuduClientTableAdmin";

  private static final Schema basicSchema =
      org.apache.kudu.test.ClientTestUtil.getBasicSchema();

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
    asyncClient = harness.getAsyncClient();
  }

  /**
   * Test creating and deleting a table through a KuduClient.
   */
  @Test(timeout = 100000)
  public void testCreateDeleteTable() throws Exception {
    // Check that we can create a table.
    client.createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());
    assertFalse(client.getTablesList().getTablesList().isEmpty());
    assertTrue(client.getTablesList().getTablesList().contains(TABLE_NAME));

    // Check that we can delete it.
    client.deleteTable(TABLE_NAME);
    assertFalse(client.getTablesList().getTablesList().contains(TABLE_NAME));

    // Check that we can re-recreate it, with a different schema.
    List<ColumnSchema> columns = new ArrayList<>(basicSchema.getColumns());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("one more", Type.STRING).build());
    Schema newSchema = new Schema(columns);
    client.createTable(TABLE_NAME, newSchema, getBasicCreateTableOptions());

    // Check that we can open a table and see that it has the new schema.
    KuduTable table = client.openTable(TABLE_NAME);
    assertEquals(newSchema.getColumnCount(), table.getSchema().getColumnCount());
    assertTrue(table.getPartitionSchema().isSimpleRangePartitioning());

    // Check that the block size parameter we specified in the schema is respected.
    assertEquals(4096, newSchema.getColumn("column3_s").getDesiredBlockSize());
    assertEquals(ColumnSchema.Encoding.DICT_ENCODING,
                 newSchema.getColumn("column3_s").getEncoding());
    assertEquals(ColumnSchema.CompressionAlgorithm.LZ4,
                 newSchema.getColumn("column3_s").getCompressionAlgorithm());
  }

  /**
   * Test recalling a soft deleted table through a KuduClient.
   */
  @Test(timeout = 100000)
  public void testRecallDeletedTable() throws Exception {
    // Check that we can create a table.
    assertTrue(client.getTablesList().getTablesList().isEmpty());
    final KuduTable table = client.createTable(TABLE_NAME, basicSchema,
        getBasicCreateTableOptions());
    final String tableId = table.getTableId();
    assertEquals(1, client.getTablesList().getTablesList().size());
    assertEquals(TABLE_NAME, client.getTablesList().getTablesList().get(0));

    // Check that we can delete it.
    client.deleteTable(TABLE_NAME, 600);
    List<String> tables = client.getTablesList().getTablesList();
    assertEquals(0, tables.size());
    tables = client.getSoftDeletedTablesList().getTablesList();
    assertEquals(1, tables.size());
    String softDeletedTable = tables.get(0);
    assertEquals(TABLE_NAME, softDeletedTable);
    // Check that we can recall the soft_deleted table.
    client.recallDeletedTable(tableId);
    assertEquals(1, client.getTablesList().getTablesList().size());
    assertEquals(TABLE_NAME, client.getTablesList().getTablesList().get(0));

    // Check that we can delete it.
    client.deleteTable(TABLE_NAME, 600);
    tables = client.getTablesList().getTablesList();
    assertEquals(0, tables.size());
    tables = client.getSoftDeletedTablesList().getTablesList();
    assertEquals(1, tables.size());
    softDeletedTable = tables.get(0);
    assertEquals(TABLE_NAME, softDeletedTable);
    // Check we can recall soft deleted table with new table name.
    final String newTableName = "NewTable";
    client.recallDeletedTable(tableId, newTableName);
    assertEquals(1, client.getTablesList().getTablesList().size());
    assertEquals(newTableName, client.getTablesList().getTablesList().get(0));
  }

  /**
   * Test creating a table with various invalid schema cases.
   */
  @Test(timeout = 100000)
  public void testCreateTableTooManyColumns() throws Exception {
    List<ColumnSchema> cols = new ArrayList<>();
    cols.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING)
             .key(true)
             .build());
    for (int i = 0; i < 1000; i++) {
      // not null with default
      cols.add(new ColumnSchema.ColumnSchemaBuilder("c" + i, Type.STRING)
               .build());
    }
    Schema schema = new Schema(cols);
    try {
      client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());
      fail();
    } catch (NonRecoverableException nre) {
      org.hamcrest.MatcherAssert.assertThat(nre.toString(),
          org.hamcrest.CoreMatchers.containsString(
              "number of columns 1001 is greater than the permitted maximum"));
    }
  }

  /**
   * Test creating and deleting a table with extra-configs through a KuduClient.
   */
  @Test(timeout = 100000)
  public void testCreateDeleteTableWitExtraConfigs() throws Exception {
    // Check that we can create a table.
    java.util.Map<String, String> extraConfigs = new java.util.HashMap<>();
    extraConfigs.put("kudu.table.history_max_age_sec", "7200");

    client.createTable(
        TABLE_NAME,
        basicSchema,
        getBasicCreateTableOptions().setExtraConfigs(extraConfigs));

    KuduTable table = client.openTable(TABLE_NAME);
    extraConfigs = table.getExtraConfig();
    assertTrue(extraConfigs.containsKey("kudu.table.history_max_age_sec"));
    assertEquals("7200", extraConfigs.get("kudu.table.history_max_age_sec"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoDefaultPartitioning() throws Exception {
    client.createTable(TABLE_NAME, basicSchema, new CreateTableOptions());
  }

  @Test(timeout = 100000)
  public void testOpenTableClearsNonCoveringRangePartitions() throws KuduException {
    CreateTableOptions options = getBasicCreateTableOptions();
    PartialRow lower = basicSchema.newPartialRow();
    PartialRow upper = basicSchema.newPartialRow();
    lower.addInt("key", 0);
    upper.addInt("key", 1);
    options.addRangePartition(lower, upper);

    client.createTable(TABLE_NAME, basicSchema, options);
    KuduTable table = client.openTable(TABLE_NAME);

    // Count the number of tablets.
    KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);
    List<KuduScanToken> tokens = tokenBuilder.build();
    assertEquals(1, tokens.size());

    // Add a range partition with a separate client. The new client is necessary
    // in order to avoid clearing the meta cache as part of the alter operation.
    try (KuduClient alterClient =
             new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString())
                 .defaultAdminOperationTimeoutMs(
                     org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP)
                 .build()) {
      lower = basicSchema.newPartialRow();
      upper = basicSchema.newPartialRow();
      lower.addInt("key", 1);
      AlterTableOptions alter = new AlterTableOptions();
      alter.addRangePartition(lower, upper);
      alterClient.alterTable(TABLE_NAME, alter);
    }

    // Count the number of tablets.  The result should still be the same, since
    // the new tablet is still cached as a non-covered range.
    tokenBuilder = client.newScanTokenBuilder(table);
    tokens = tokenBuilder.build();
    assertEquals(1, tokens.size());

    // Reopen the table and count the tablets again. The new tablet should now show up.
    table = client.openTable(TABLE_NAME);
    tokenBuilder = client.newScanTokenBuilder(table);
    tokens = tokenBuilder.build();
    assertEquals(2, tokens.size());
  }

  @Test(timeout = 100000)
  public void testCreateTableWithConcurrentInsert() throws Exception {
    KuduTable table = client.createTable(
        TABLE_NAME, createManyStringsSchema(), getBasicCreateTableOptions().setWait(false));

    // Insert a row.
    // It's very likely that the tablets are still being created, but the client
    // should transparently retry the insert (and associated master lookup)
    // until the operation succeeds.
    Insert insert = table.newInsert();
    insert.getRow().addString("key", "key_0");
    insert.getRow().addString("c1", "c1_0");
    insert.getRow().addString("c2", "c2_0");
    KuduSession session = client.newSession();
    OperationResponse resp = session.apply(insert);
    assertFalse(resp.hasRowError());

    // This won't do anything useful (i.e. if the insert succeeds, we know the
    // table has been created), but it's here for additional code coverage.
    assertTrue(client.isCreateTableDone(TABLE_NAME));
  }

  @Test(timeout = 100000)
  public void testCreateTableWithConcurrentAlter() throws Exception {
    // Kick off an asynchronous table creation.
    Deferred<KuduTable> d = asyncClient.createTable(TABLE_NAME,
        createManyStringsSchema(), getBasicCreateTableOptions());

    // Rename the table that's being created to make sure it doesn't interfere
    // with the "wait for all tablets to be created" behavior of createTable().
    // We have to retry this in a loop because we might run before the table
    // actually exists.
    while (true) {
      try {
        client.alterTable(TABLE_NAME,
            new AlterTableOptions().renameTable("foo"));
        break;
      } catch (KuduException e) {
        if (!e.getStatus().isNotFound()) {
          throw e;
        }
      }
    }

    // If createTable() was disrupted by the alterTable(), this will throw.
    d.join();
  }
}


