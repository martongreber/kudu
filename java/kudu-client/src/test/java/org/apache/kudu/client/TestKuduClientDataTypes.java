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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kudu.test.ClientTestUtil.createManyStringsSchema;
import static org.apache.kudu.test.ClientTestUtil.createManyVarcharsSchema;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithBinaryColumns;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithDateColumns;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithDecimalColumns;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithTimestampColumns;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.Schema;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.util.DateUtil;
import org.apache.kudu.util.DecimalUtil;
import org.apache.kudu.util.TimestampUtil;

public class TestKuduClientDataTypes {
  private static final String TABLE_NAME = "TestKuduClientDataTypes";

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
  public void testVarchars() throws Exception {
    Schema schema = createManyVarcharsSchema();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addVarchar("key", String.format("key_%02d", i));
      row.addVarchar("c2", "c2_" + i);
      if (i % 2 == 1) {
        row.addVarchar("c3", "c3_" + i);
      }
      row.addVarchar("c4", "c4_" + i);
      row.addVarchar("c1", "c1_" + i);
      session.apply(insert);
      if (i % 50 == 0) {
        session.flush();
      }
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(100, rowStrings.size());
    assertEquals(
        "VARCHAR key(10)=key_03, VARCHAR c1(10)=c1_3, VARCHAR c2(10)=c2_3,"
        + " VARCHAR c3(10)=c3_3, VARCHAR c4(10)=c4_3", rowStrings.get(3));
    assertEquals(
        "VARCHAR key(10)=key_04, VARCHAR c1(10)=c1_4, VARCHAR c2(10)=c2_4,"
        + " VARCHAR c3(10)=NULL, VARCHAR c4(10)=c4_4", rowStrings.get(4));

    KuduScanner scanner = client.newScannerBuilder(table).build();

    assertTrue("Scanner should have returned row", scanner.hasMoreRows());

    RowResultIterator rows = scanner.nextRows();
    final RowResult next = rows.next();

    try {
      next.getInt("c2");
      fail("IllegalArgumentException was not thrown when accessing "
          + "a VARCHAR column with getInt");
    } catch (IllegalArgumentException ignored) {
    }
  }

  @Test(timeout = 100000)
  public void testStrings() throws Exception {
    Schema schema = createManyStringsSchema();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addString("key", String.format("key_%02d", i));
      row.addString("c2", "c2_" + i);
      if (i % 2 == 1) {
        row.addString("c3", "c3_" + i);
      }
      row.addString("c4", "c4_" + i);
      row.addString("c1", "c1_" + i);
      session.apply(insert);
      if (i % 50 == 0) {
        session.flush();
      }
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(100, rowStrings.size());
    assertEquals(
        "STRING key=key_03, STRING c1=c1_3, STRING c2=c2_3, STRING c3=c3_3, STRING c4=c4_3",
        rowStrings.get(3));
    assertEquals(
        "STRING key=key_04, STRING c1=c1_4, STRING c2=c2_4, STRING c3=NULL, STRING c4=c4_4",
        rowStrings.get(4));

    KuduScanner scanner = client.newScannerBuilder(table).build();

    assertTrue("Scanner should have returned row", scanner.hasMoreRows());

    RowResultIterator rows = scanner.nextRows();
    final RowResult next = rows.next();

    try {
      next.getInt("c2");
      fail("IllegalArgumentException was not thrown when accessing "
          + "a string column with getInt");
    } catch (IllegalArgumentException ignored) {
    }
  }

  @Test(timeout = 100000)
  public void testUTF8() throws Exception {
    Schema schema = createManyStringsSchema();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduTable table = client.openTable(TABLE_NAME);
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addString("key", "กขฃคฅฆง");
    row.addString("c1", "✁✂✃✄✆");
    row.addString("c2", "hello");
    row.addString("c4", "🐱");
    KuduSession session = client.newSession();
    session.apply(insert);
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(1, rowStrings.size());
    assertEquals(
        "STRING key=กขฃคฅฆง, STRING c1=✁✂✃✄✆, STRING c2=hello, STRING c3=NULL, STRING c4=🐱",
        rowStrings.get(0));
  }

  @Test(timeout = 100000)
  public void testBinaryColumns() throws Exception {
    Schema schema = createSchemaWithBinaryColumns();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    byte[] testArray = new byte[] {1, 2, 3, 4, 5, 6 ,7, 8, 9};

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addBinary("key", String.format("key_%02d", i).getBytes(UTF_8));
      row.addString("c1", "✁✂✃✄✆");
      row.addDouble("c2", i);
      if (i % 2 == 1) {
        row.addBinary("c3", testArray);
      }
      session.apply(insert);
      if (i % 50 == 0) {
        session.flush();
      }
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(100, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append(String.format("BINARY key=\"key_%02d\", STRING c1=✁✂✃✄✆, DOUBLE c2=%.1f,"
          + " BINARY c3=", i, (double) i));
      if (i % 2 == 1) {
        expectedRow.append(Bytes.pretty(testArray));
      } else {
        expectedRow.append("NULL");
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }
  }

  @Test(timeout = 100000)
  public void testTimestampColumns() throws Exception {
    Schema schema = createSchemaWithTimestampColumns();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    List<Long> timestamps = new ArrayList<>();

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    long lastTimestamp = 0;
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      long timestamp = System.currentTimeMillis() * 1000;
      while (timestamp == lastTimestamp) {
        timestamp = System.currentTimeMillis() * 1000;
      }
      timestamps.add(timestamp);
      row.addLong("key", timestamp);
      if (i % 2 == 1) {
        row.addLong("c1", timestamp);
      }
      session.apply(insert);
      if (i % 50 == 0) {
        session.flush();
      }
      lastTimestamp = timestamp;
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(100, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append(String.format("UNIXTIME_MICROS key=%s, UNIXTIME_MICROS c1=",
          TimestampUtil.timestampToString(timestamps.get(i))));
      if (i % 2 == 1) {
        expectedRow.append(TimestampUtil.timestampToString(timestamps.get(i)));
      } else {
        expectedRow.append("NULL");
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }
  }

  @Test(timeout = 100000)
  public void testDateColumns() throws Exception {
    Schema schema = createSchemaWithDateColumns();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    List<Integer> dates = new ArrayList<>();

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      dates.add(i);
      Date date = DateUtil.epochDaysToSqlDate(i);
      row.addDate("key", date);
      if (i % 2 == 1) {
        row.addDate("c1", date);
      }
      session.apply(insert);
      if (i % 50 == 0) {
        session.flush();
      }
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(100, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      String sdate = DateUtil.epochDaysToDateString(dates.get(i));
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append(String.format("DATE key=%s, DATE c1=", sdate));
      if (i % 2 == 1) {
        expectedRow.append(sdate);
      } else {
        expectedRow.append("NULL");
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }
  }

  @Test(timeout = 100000)
  public void testDecimalColumns() throws Exception {
    Schema schema = createSchemaWithDecimalColumns();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    assertEquals(DecimalUtil.MAX_DECIMAL128_PRECISION,
        table.getSchema().getColumn("c1").getTypeAttributes().getPrecision());

    for (int i = 0; i < 9; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addDecimal("key", BigDecimal.valueOf(i));
      if (i % 2 == 1) {
        row.addDecimal("c1", BigDecimal.valueOf(i));
      }
      session.apply(insert);
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(9, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append(String.format("DECIMAL key(18, 0)=%s, DECIMAL c1(38, 0)=",
          String.valueOf(i)));
      if (i % 2 == 1) {
        expectedRow.append(i);
      } else {
        expectedRow.append("NULL");
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }
  }
}


