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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.KuduTestHarness;

public class TestKuduClientAutoIncrement {
  private static final String TABLE_NAME = "TestKuduClientAutoIncrement";

  private KuduClient client;
  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
  }

  @Test(timeout = 100000)
  public void testCreateTableWithNonUniquePrimaryKeys() throws Exception {
    Schema schema = org.apache.kudu.test.ClientTestUtil.createSchemaWithNonUniqueKey();
    assertFalse(schema.isPrimaryKeyUnique());
    assertTrue(schema.hasAutoIncrementingColumn());
    assertEquals(3, schema.getColumnCount());
    assertEquals(2, schema.getPrimaryKeyColumnCount());
    assertEquals(1, schema.getColumnIndex(Schema.getAutoIncrementingColumnName()));
    client.createTable(TABLE_NAME, schema, org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    schema = table.getSchema();
    assertFalse(schema.isPrimaryKeyUnique());
    assertTrue(schema.hasAutoIncrementingColumn());
    assertEquals(3, schema.getColumnCount());
    assertEquals(2, schema.getPrimaryKeyColumnCount());
    assertEquals(1, schema.getColumnIndex(Schema.getAutoIncrementingColumnName()));
    assertTrue(schema.getColumn(Schema.getAutoIncrementingColumnName()).isKey());
    assertTrue(schema.getColumn(
        Schema.getAutoIncrementingColumnName()).isAutoIncrementing());

    for (int i = 0; i < 3; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addInt("key", i);
      row.addInt("c1", i * 10);
      session.apply(insert);
    }
    session.flush();

    List<String> rowStrings = org.apache.kudu.test.ClientTestUtil.scanTableToStrings(table);
    assertEquals(3, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append(String.format("INT32 key=%d, INT64 %s=%d, INT32 c1=%d",
          i, Schema.getAutoIncrementingColumnName(), i + 1, i * 10));
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }

    Update update = table.newUpdate();
    PartialRow row = update.getRow();
    row.addInt(schema.getColumnByIndex(0).getName(), 0);
    row.addLong(schema.getColumnByIndex(1).getName(), 1);
    row.addInt(schema.getColumnByIndex(2).getName(), 100);
    session.apply(update);
    session.flush();

    KuduScanner.KuduScannerBuilder scanBuilder = client.newScannerBuilder(table);
    KuduScanner scanner =
        scanBuilder.setProjectedColumnNames(Lists.newArrayList("key", "c1")).build();
    rowStrings.clear();
    for (RowResult r : scanner) {
      rowStrings.add(r.rowToString());
    }
    Collections.sort(rowStrings);
    assertEquals(3, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      if (i == 0) {
        expectedRow.append(String.format("INT32 key=0, INT32 c1=100"));
      } else {
        expectedRow.append(String.format("INT32 key=%d, INT32 c1=%d", i, i * 10));
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }

    for (int i = 0; i < 6; i++) {
      Upsert upsert = table.newUpsert();
      row = upsert.getRow();
      row.addInt("key", i);
      row.addLong(Schema.getAutoIncrementingColumnName(), i + 1);
      row.addInt("c1", i * 20);
      session.apply(upsert);
    }
    session.flush();

    rowStrings = org.apache.kudu.test.ClientTestUtil.scanTableToStrings(table);
    assertEquals(6, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      String expectedRow = String.format("INT32 key=%d, INT64 %s=%d, INT32 c1=%d",
              i, Schema.getAutoIncrementingColumnName(), i + 1, i * 20);
      assertEquals(expectedRow, rowStrings.get(i));
    }

    Delete delete = table.newDelete();
    row = delete.getRow();
    row.addInt(schema.getColumnByIndex(0).getName(), 0);
    row.addLong(schema.getColumnByIndex(1).getName(), 1);
    session.apply(delete);
    session.flush();
    assertEquals(5, org.apache.kudu.test.ClientTestUtil.countRowsInScan(client.newScannerBuilder(table).build()));

    client.deleteTable(TABLE_NAME);
  }

  @Test(timeout = 100000)
  public void testTableWithAutoIncrementingColumn() throws Exception {
    Schema schema = org.apache.kudu.test.ClientTestUtil.createSchemaWithNonUniqueKey();
    assertFalse(schema.isPrimaryKeyUnique());
    assertTrue(schema.hasAutoIncrementingColumn());
    assertEquals(3, schema.getColumnCount());
    assertEquals(2, schema.getPrimaryKeyColumnCount());
    client.createTable(TABLE_NAME, schema, org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions());

    final KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    schema = table.getSchema();
    assertTrue(schema.hasAutoIncrementingColumn());

    Upsert upsert = table.newUpsert();
    PartialRow rowUpsert = upsert.getRow();
    rowUpsert.addInt("key", 0);
    rowUpsert.addLong(Schema.getAutoIncrementingColumnName(), 1);
    rowUpsert.addInt("c1", 10);
    session.apply(upsert);

    UpsertIgnore upsertIgnore = table.newUpsertIgnore();
    PartialRow rowUpsertIgnore = upsertIgnore.getRow();
    rowUpsertIgnore.addInt("key", 1);
    rowUpsertIgnore.addLong(Schema.getAutoIncrementingColumnName(), 2);
    rowUpsertIgnore.addInt("c1", 20);
    session.apply(upsertIgnore);

    client.alterTable(TABLE_NAME, new AlterTableOptions().changeDesiredBlockSize(
        Schema.getAutoIncrementingColumnName(), 1));
    client.alterTable(TABLE_NAME, new AlterTableOptions().changeEncoding(
        Schema.getAutoIncrementingColumnName(), ColumnSchema.Encoding.PLAIN_ENCODING));
    client.alterTable(TABLE_NAME, new AlterTableOptions().changeCompressionAlgorithm(
        Schema.getAutoIncrementingColumnName(), ColumnSchema.CompressionAlgorithm.NO_COMPRESSION));
    session.flush();

    try {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addInt("key", 1);
      row.addLong(Schema.getAutoIncrementingColumnName(), 1);
      row.addInt("c1", 10);
      session.apply(insert);
      fail("INSERT on table with auto-incrementing column set");
    } catch (KuduException ex) {
      assertTrue(ex.getMessage().contains("Auto-Incrementing column should not " +
          "be specified for INSERT operation"));
    }

    try {
      InsertIgnore insertIgnore = table.newInsertIgnore();
      PartialRow row = insertIgnore.getRow();
      row.addInt("key", 1);
      row.addLong(Schema.getAutoIncrementingColumnName(), 1);
      row.addInt("c1", 10);
      session.apply(insertIgnore);
      fail("INSERT on table with auto-incrementing column set");
    } catch (KuduException ex) {
      assertTrue(ex.getMessage().contains("Auto-Incrementing column should not " +
          "be specified for INSERT operation"));
    }
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().addColumn(
          Schema.getAutoIncrementingColumnName(), Schema.getAutoIncrementingColumnType(), 0));
      fail("Add auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Column name " +
          Schema.getAutoIncrementingColumnName() + " is reserved by Kudu engine"));
    }
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().addColumn(
          new ColumnSchema.AutoIncrementingColumnSchemaBuilder().build()));
      fail("Add auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Column name " +
          Schema.getAutoIncrementingColumnName() + " is reserved by Kudu engine"));
    }

    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().dropColumn(
          Schema.getAutoIncrementingColumnName()));
      fail("Drop auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot remove auto-incrementing column " +
          Schema.getAutoIncrementingColumnName()));
    }

    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().renameColumn(
          Schema.getAutoIncrementingColumnName(), "new_auto_incrementing"));
      fail("Rename auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot rename auto-incrementing column " +
          Schema.getAutoIncrementingColumnName()));
    }

    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().removeDefault(
          Schema.getAutoIncrementingColumnName()));
      fail("Remove default value for auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Auto-incrementing column " +
          Schema.getAutoIncrementingColumnName() + " does not have default value"));
    }

    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().changeDefault(
          Schema.getAutoIncrementingColumnName(), 0));
      fail("Change default value for auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot set default value for " +
          "auto-incrementing column " + Schema.getAutoIncrementingColumnName()));
    }

    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().changeImmutable(
          Schema.getAutoIncrementingColumnName(), true));
      fail("Change immutable for auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot change immutable for " +
          "auto-incrementing column " + Schema.getAutoIncrementingColumnName()));
    }

    client.deleteTable(TABLE_NAME);
  }
}


