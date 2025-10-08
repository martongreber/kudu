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

import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER_EQUAL;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS;
import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getBasicTableOptionsWithNonCoveredRange;
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import io.netty.util.Timeout;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig;
import org.apache.kudu.test.RandomUtils;

public class TestKuduClientScannerLiveness {
  private static final String TABLE_NAME = "TestKuduClientScannerLiveness";

  private static final int SHORT_SCANNER_TTL_MS = 5000;
  private static final int SHORT_SCANNER_GC_US = SHORT_SCANNER_TTL_MS * 100; // 10% of the TTL.

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

  @Test(timeout = 100000)
  @TabletServerConfig(flags = {
      "--scanner_ttl_ms=" + SHORT_SCANNER_TTL_MS,
      "--scanner_gc_check_interval_us=" + SHORT_SCANNER_GC_US,
  })
  public void testScannerExpiration() throws Exception {
    int numRows = 1000;
    client.createTable(
        TABLE_NAME,
        basicSchema,
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));
    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    for (int i = 0; i < numRows; i++) {
      Insert insert = org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert(table, i);
      session.apply(insert);
    }

    KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
        .replicaSelection(ReplicaSelection.CLOSEST_REPLICA)
        .batchSizeBytes(100)
        .build();

    int rows = scanner.nextRows().getNumRows();
    assertTrue("Scanner did not read any rows", rows > 0);

    Thread.sleep(SHORT_SCANNER_TTL_MS * 2);

    try {
      scanner.nextRows();
      fail("Exception was not thrown when accessing an expired scanner");
    } catch (NonRecoverableException ex) {
      assertTrue("Expected Scanner not found error, got:\n" + ex.toString(),
                 ex.getMessage().matches(".*Scanner .* not found.*"));
    }

    scanner.close();
  }

  @Test(timeout = 100000)
  @TabletServerConfig(flags = {
      "--scanner_ttl_ms=" + SHORT_SCANNER_TTL_MS,
      "--scanner_gc_check_interval_us=" + SHORT_SCANNER_GC_US,
  })
  public void testKeepAlive() throws Exception {
    int numRows = 1000;
    client.createTable(
        TABLE_NAME,
        basicSchema,
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));
    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    for (int i = 0; i < numRows; i++) {
      Insert insert = org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert(table, i);
      session.apply(insert);
    }

    KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
        .replicaSelection(ReplicaSelection.CLOSEST_REPLICA)
        .batchSizeBytes(100)
        .build();

    scanner.keepAlive();
    int accum = scanner.nextRows().getNumRows();

    while (scanner.hasMoreRows()) {
      int rows = scanner.nextRows().getNumRows();
      accum += rows;
      if (scanner.currentTablet() == null) {
        org.slf4j.LoggerFactory.getLogger(getClass())
            .info(String.format("Between tablets after scanning %d rows", accum));
        break;
      }
      if (accum == numRows) {
        fail("All rows were in a single tablet.");
      }
    }

    scanner.keepAlive();
    accum += scanner.nextRows().getNumRows();

    Random random = RandomUtils.getRandom();
    for (int i = 0; i < 10; i++) {
      Thread.sleep(SHORT_SCANNER_TTL_MS / 4);
      if (i % 3 == 0) {
        RpcProxy.failNextRpcs(random.nextInt(4),
            new RecoverableException(Status.ServiceUnavailable("testKeepAlive")));
      }
      scanner.keepAlive();
    }

    while (scanner.hasMoreRows()) {
      accum += scanner.nextRows().getNumRows();
    }
    assertEquals("All rows were not scanned", numRows, accum);

    try {
      scanner.keepAlive();
      fail("Exception was not thrown when calling keepAlive on a closed scanner");
    } catch (IllegalStateException ex) {
      org.hamcrest.MatcherAssert.assertThat(ex.getMessage(),
          org.hamcrest.CoreMatchers.containsString("Scanner has already been closed"));
    }
  }

  @Test(timeout = 100000)
  @TabletServerConfig(flags = {
      "--scanner_ttl_ms=" + SHORT_SCANNER_TTL_MS / 5,
      "--scanner_gc_check_interval_us=" + SHORT_SCANNER_GC_US,
  })
  public void testKeepAlivePeriodically() throws Exception {
    int numRows = 1000;
    client.createTable(
        TABLE_NAME,
        basicSchema,
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 3));
    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    for (int i = 0; i < numRows; i++) {
      Insert insert = org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert(table, i);
      session.apply(insert);
    }

    {
      KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
          .replicaSelection(ReplicaSelection.CLOSEST_REPLICA)
          .batchSizeBytes(100)
          .build();

      scanner.startKeepAlivePeriodically(SHORT_SCANNER_TTL_MS / 10);
      int rowCount = 0;
      while (scanner.hasMoreRows()) {
        Thread.sleep(SHORT_SCANNER_TTL_MS / 2);
        rowCount += scanner.nextRows().getNumRows();
      }
      assertEquals(numRows, rowCount);
      Field fieldAsyncScanner = KuduScanner.class.getDeclaredField("asyncScanner");
      fieldAsyncScanner.setAccessible(true);
      AsyncKuduScanner asyncScanner = (AsyncKuduScanner)fieldAsyncScanner.get(scanner);
      Field fieldKeepaliveTimeout =
          AsyncKuduScanner.class.getDeclaredField("keepAliveTimeout");
      fieldKeepaliveTimeout.setAccessible(true);
      Timeout keepAliveTimeout = (Timeout)fieldKeepaliveTimeout.get(asyncScanner);
      assertTrue(keepAliveTimeout.isCancelled());
    }

    {
      KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
          .replicaSelection(ReplicaSelection.CLOSEST_REPLICA)
          .batchSizeBytes(100)
          .build();

      scanner.startKeepAlivePeriodically(SHORT_SCANNER_TTL_MS / 10);

      Field fieldAsyncScanner = KuduScanner.class.getDeclaredField("asyncScanner");
      fieldAsyncScanner.setAccessible(true);
      AsyncKuduScanner asyncScanner = (AsyncKuduScanner)fieldAsyncScanner.get(scanner);
      Field fieldKeepaliveTimeout =
          AsyncKuduScanner.class.getDeclaredField("keepAliveTimeout");
      fieldKeepaliveTimeout.setAccessible(true);
      Timeout keepAliveTimeout = (Timeout)fieldKeepaliveTimeout.get(asyncScanner);
      assertFalse(keepAliveTimeout.isCancelled());

      scanner.close();
      assertTrue(keepAliveTimeout.isCancelled());
    }
  }

  @Test(timeout = 100000)
  @TabletServerConfig(flags = {
      "--scanner_ttl_ms=" + SHORT_SCANNER_TTL_MS / 5,
      "--scanner_gc_check_interval_us=" + SHORT_SCANNER_GC_US,
  })
  public void testStopKeepAlivePeriodically() throws Exception {
    int numRows = 1000;
    client.createTable(
        TABLE_NAME,
        basicSchema,
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 3));
    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    for (int i = 0; i < numRows; i++) {
      Insert insert = org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert(table, i);
      session.apply(insert);
    }

    KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
        .replicaSelection(ReplicaSelection.CLOSEST_REPLICA)
        .batchSizeBytes(100)
        .build();
    assertTrue(scanner.startKeepAlivePeriodically(SHORT_SCANNER_TTL_MS / 10));
    assertTrue(scanner.stopKeepAlivePeriodically());
    while (scanner.hasMoreRows()) {
      try {
        Thread.sleep(SHORT_SCANNER_TTL_MS / 2);
        scanner.nextRows();
      } catch (Exception e) {
        assertTrue(e.toString().contains("not found (it may have expired)"));
        break;
      }
    }
  }

  @Test
  public void testScanWithLimit() throws Exception {
    AsyncKuduClient asyncClient = harness.getAsyncClient();
    client.createTable(TABLE_NAME, basicSchema, getBasicTableOptionsWithNonCoveredRange());
    KuduTable table = client.openTable(TABLE_NAME);
    KuduSession session = client.newSession();
    int numRows = 100;
    for (int key = 0; key < numRows; key++) {
      session.apply(org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert(table, key));
    }

    int[] nonPositives = { -1, 0 };
    for (int limit : nonPositives) {
      try {
        client.newScannerBuilder(table).limit(limit).build();
        fail();
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage().contains("Need a strictly positive number"));
      }
    }

    int[] limits = { numRows - 1, numRows, numRows + 1 };
    for (int limit : limits) {
      KuduScanner scanner = client.newScannerBuilder(table)
                                      .limit(limit)
                                      .build();
      int count = 0;
      while (scanner.hasMoreRows()) {
        count += scanner.nextRows().getNumRows();
      }
      assertEquals(String.format("Limit %d returned %d/%d rows", limit, count, numRows),
          Math.min(numRows, limit), count);
    }

    for (int limit : limits) {
      AsyncKuduScanner scanner = new AsyncKuduScanner.AsyncKuduScannerBuilder(asyncClient, table)
                                                     .limit(limit)
                                                     .build();
      assertEquals(Math.min(limit, numRows), countRowsInScan(scanner));
    }
  }

  @Test
  public void testScanWithPredicates() throws Exception {
    Schema schema = org.apache.kudu.test.ClientTestUtil.createManyStringsSchema();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    KuduTable table = client.openTable(TABLE_NAME);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addString("key", String.format("key_%02d", i));
      row.addString("c1", "c1_" + i);
      row.addString("c2", "c2_" + i);
      if (i % 2 == 0) {
        row.addString("c3", "c3_" + i);
      }
      session.apply(insert);
    }
    session.flush();

    assertEquals(100, scanTableToStrings(table).size());
    assertEquals(50, scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(schema.getColumn("key"), GREATER_EQUAL, "key_50")
    ).size());
    assertEquals(25, scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(schema.getColumn("key"), GREATER, "key_74")
    ).size());
    assertEquals(25, scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(schema.getColumn("key"), GREATER, "key_24"),
        KuduPredicate.newComparisonPredicate(schema.getColumn("c1"),
            org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS_EQUAL, "c1_49")
    ).size());
    assertEquals(50, scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(schema.getColumn("key"), GREATER, "key_24"),
        KuduPredicate.newComparisonPredicate(schema.getColumn("key"), GREATER_EQUAL, "key_50")
    ).size());
    assertEquals(0, scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(schema.getColumn("c1"),
            org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER, "c1_30"),
        KuduPredicate.newComparisonPredicate(schema.getColumn("c2"),
            org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS, "c2_20")
    ).size());
    assertEquals(0, scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(schema.getColumn("c2"),
            org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER, "c2_30"),
        KuduPredicate.newComparisonPredicate(schema.getColumn("c2"),
            org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS, "c2_20")
    ).size());

    assertEquals(100, scanTableToStrings(table,
        KuduPredicate.newIsNotNullPredicate(schema.getColumn("c1")),
        KuduPredicate.newIsNotNullPredicate(schema.getColumn("key"))
    ).size());
    assertEquals(50, scanTableToStrings(table,
        KuduPredicate.newIsNotNullPredicate(schema.getColumn("c3"))
    ).size());

    assertEquals(0, scanTableToStrings(table,
        KuduPredicate.newIsNullPredicate(schema.getColumn("c2")),
        KuduPredicate.newIsNullPredicate(schema.getColumn("key"))
    ).size());
    assertEquals(50, scanTableToStrings(table,
        KuduPredicate.newIsNullPredicate(schema.getColumn("c3"))
    ).size());

    assertEquals(3, scanTableToStrings(table,
        KuduPredicate.newInListPredicate(schema.getColumn("key"),
                                         ImmutableList.of("key_30", "key_01", "invalid", "key_99"))
    ).size());
    assertEquals(3, scanTableToStrings(table,
        KuduPredicate.newInListPredicate(schema.getColumn("c2"),
                                         ImmutableList.of("c2_30", "c2_1", "invalid", "c2_99"))
    ).size());
    assertEquals(2, scanTableToStrings(table,
        KuduPredicate.newInListPredicate(schema.getColumn("c2"),
                                         ImmutableList.of("c2_30", "c2_1", "invalid", "c2_99")),
        KuduPredicate.newIsNotNullPredicate(schema.getColumn("c2")),
        KuduPredicate.newInListPredicate(schema.getColumn("key"),
                                         ImmutableList.of("key_30", "key_45", "invalid", "key_99"))
    ).size());
  }

  @Test(timeout = 100000)
  public void testRangeWithCustomHashSchema() throws Exception {
    java.util.List<org.apache.kudu.ColumnSchema> cols = new java.util.ArrayList<>();
    cols.add(new org.apache.kudu.ColumnSchema.ColumnSchemaBuilder("c0", org.apache.kudu.Type.INT64)
        .key(true).build());
    cols.add(new org.apache.kudu.ColumnSchema.ColumnSchemaBuilder("c1", org.apache.kudu.Type.INT32)
        .nullable(true).build());
    Schema schema = new Schema(cols);

    CreateTableOptions options = new CreateTableOptions();
    options.setRangePartitionColumns(ImmutableList.of("c0"));
    options.addHashPartitions(ImmutableList.of("c0"), 2);

    {
      PartialRow lower = schema.newPartialRow();
      lower.addLong("c0", -100);
      PartialRow upper = schema.newPartialRow();
      upper.addLong("c0", 100);
      options.addRangePartition(lower, upper);
    }

    {
      PartialRow lower = schema.newPartialRow();
      lower.addLong("c0", 100);
      PartialRow upper = schema.newPartialRow();
      upper.addLong("c0", 200);

      RangePartitionWithCustomHashSchema rangePartition =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      rangePartition.addHashPartitions(ImmutableList.of("c0"), 5, 0);
      options.addRangePartition(rangePartition);
    }

    client.createTable(TABLE_NAME, schema, options);

    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
    KuduTable table = client.openTable(TABLE_NAME);

    {
      for (int i = 0; i < 10; ++i) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addLong("c0", i);
        row.addInt("c1", 1000 * i);
        session.apply(insert);
      }

      List<String> rowStringsAll = scanTableToStrings(table);
      assertEquals(10, rowStringsAll.size());

      List<String> rowStrings = scanTableToStrings(table,
          KuduPredicate.newComparisonPredicate(schema.getColumn("c0"), GREATER_EQUAL, 0),
          KuduPredicate.newComparisonPredicate(schema.getColumn("c0"), LESS, 100));
      assertEquals(10, rowStrings.size());
      for (int i = 0; i < rowStrings.size(); ++i) {
        StringBuilder expectedRow = new StringBuilder();
        expectedRow.append(String.format("INT64 c0=%d, INT32 c1=%d", i, 1000 * i));
        assertEquals(expectedRow.toString(), rowStrings.get(i));
      }
    }

    {
      for (int i = 100; i < 110; ++i) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addLong("c0", i);
        row.addInt("c1", 2 * i);
        session.apply(insert);
      }

      List<String> rowStringsAll = scanTableToStrings(table);
      assertEquals(20, rowStringsAll.size());

      List<String> rowStrings = scanTableToStrings(table,
          KuduPredicate.newComparisonPredicate(schema.getColumn("c0"), GREATER_EQUAL, 100));
      assertEquals(10, rowStrings.size());
      for (int i = 0; i < rowStrings.size(); ++i) {
        StringBuilder expectedRow = new StringBuilder();
        expectedRow.append(String.format("INT64 c0=%d, INT32 c1=%d",
            i + 100, 2 * (i + 100)));
        assertEquals(expectedRow.toString(), rowStrings.get(i));
      }
    }
  }

  private int countRowsForTestScanNonCoveredTable(KuduTable table,
                                                  Integer lowerBound,
                                                  Integer upperBound) throws Exception {
    KuduScanner.KuduScannerBuilder scanBuilder = client.newScannerBuilder(table);
    if (lowerBound != null) {
      PartialRow bound = basicSchema.newPartialRow();
      bound.addInt(0, lowerBound);
      scanBuilder.lowerBound(bound);
    }
    if (upperBound != null) {
      PartialRow bound = basicSchema.newPartialRow();
      bound.addInt(0, upperBound);
      scanBuilder.exclusiveUpperBound(bound);
    }

    KuduScanner scanner = scanBuilder.build();
    int count = 0;
    while (scanner.hasMoreRows()) {
      count += scanner.nextRows().getNumRows();
    }
    return count;
  }

  @Test(timeout = 100000)
  public void testScanNonCoveredTable() throws Exception {
    client.createTable(TABLE_NAME, basicSchema, getBasicTableOptionsWithNonCoveredRange());

    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    KuduTable table = client.openTable(TABLE_NAME);

    for (int key = 0; key < 100; key++) {
      session.apply(org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert(table, key));
    }
    for (int key = 200; key < 300; key++) {
      session.apply(org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert(table, key));
    }
    session.flush();
    assertEquals(0, session.countPendingErrors());

    assertEquals(200, countRowsForTestScanNonCoveredTable(table, null, null));
    assertEquals(100, countRowsForTestScanNonCoveredTable(table, null, 200));
    assertEquals(0, countRowsForTestScanNonCoveredTable(table, null, -1));
    assertEquals(0, countRowsForTestScanNonCoveredTable(table, 120, 180));
    assertEquals(0, countRowsForTestScanNonCoveredTable(table, 300, null));
  }
}


