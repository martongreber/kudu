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
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.test.CapturingLogAppender;
import org.apache.kudu.test.KuduTestHarness;

public class TestKuduClientSessionAndConsistency {
  private static final String TABLE_NAME = "TestKuduClientSessionAndConsistency";

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
  public void testTableWithDefaults() throws Exception {
    List<org.apache.kudu.ColumnSchema> cols = new ArrayList<>();
    cols.add(new org.apache.kudu.ColumnSchema.ColumnSchemaBuilder("key", org.apache.kudu.Type.STRING)
             .key(true)
             .build());
    cols.add(new org.apache.kudu.ColumnSchema.ColumnSchemaBuilder("c1", org.apache.kudu.Type.STRING)
             .nullable(true)
             .build());
    cols.add(new org.apache.kudu.ColumnSchema.ColumnSchemaBuilder("c2", org.apache.kudu.Type.STRING)
             .nullable(true)
             .defaultValue("def")
             .build());
    cols.add(new org.apache.kudu.ColumnSchema.ColumnSchemaBuilder("c3", org.apache.kudu.Type.STRING)
             .nullable(false)
             .build());
    cols.add(new org.apache.kudu.ColumnSchema.ColumnSchemaBuilder("c4", org.apache.kudu.Type.STRING)
             .nullable(false)
             .defaultValue("def")
             .build());
    Schema schema = new Schema(cols);
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());
    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    List<String> rows = ImmutableList.of(
        "r1,a,b,c,d",
        "r2,NULL,NULL,c,d",
        "r3,-,-,c,-",
        "fail_1,a,b,c,NULL",
        "fail_2,a,b,NULL,d");
    for (String row : rows) {
      try {
        String[] fields = row.split(",", -1);
        Insert insert = table.newInsert();
        for (int i = 0; i < fields.length; i++) {
          if (fields[i].equals("-")) {
            continue;
          }
          if (fields[i].equals("NULL")) {
            insert.getRow().setNull(i);
          } else {
            insert.getRow().addString(i, fields[i]);
          }
        }
        session.apply(insert);
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage(),
                   e.getMessage().matches("c[34] cannot be set to null"));
      }
    }
    session.flush();

    List<String> expectedStrings = ImmutableList.of(
        "STRING key=r1, STRING c1=a, STRING c2=b, STRING c3=c, STRING c4=d",
        "STRING key=r2, STRING c1=NULL, STRING c2=NULL, STRING c3=c, STRING c4=d",
        "STRING key=r3, STRING c1=NULL, STRING c2=def, STRING c3=c, STRING c4=def");
    List<String> rowStrings = scanTableToStrings(table);
    java.util.Collections.sort(rowStrings);
    org.junit.Assert.assertArrayEquals(rowStrings.toArray(new String[0]),
                      expectedStrings.toArray(new String[0]));
  }

  @Test(timeout = 100000)
  public void testAutoClose() throws Exception {
    try (KuduClient localClient =
             new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build()) {
      localClient.createTable(TABLE_NAME, org.apache.kudu.test.ClientTestUtil.getBasicSchema(), getBasicCreateTableOptions());
      KuduTable table = localClient.openTable(TABLE_NAME);
      KuduSession session = localClient.newSession();

      session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
      Insert insert = org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert(table, 0);
      session.apply(insert);
    }

    KuduTable table = client.openTable(TABLE_NAME);
    AsyncKuduScanner scanner =
        new AsyncKuduScanner.AsyncKuduScannerBuilder(asyncClient, table).build();
    assertEquals(1, org.apache.kudu.test.ClientTestUtil.countRowsInScan(scanner));
  }

  @Test(timeout = 100000)
  public void testSessionOnceClosed() throws Exception {
    client.createTable(TABLE_NAME, org.apache.kudu.test.ClientTestUtil.getBasicSchema(), getBasicCreateTableOptions());
    KuduTable table = client.openTable(TABLE_NAME);
    KuduSession session = client.newSession();

    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    Insert insert = org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert(table, 0);
    session.apply(insert);
    session.close();
    assertTrue(session.isClosed());

    insert = org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert(table, 1);
    CapturingLogAppender cla = new CapturingLogAppender();
    try (Closeable c = cla.attach()) {
      session.apply(insert);
    }
    String loggedText = cla.getAppendedText();
    assertTrue("Missing warning:\n" + loggedText,
               loggedText.contains("this is unsafe"));
  }

  @Test(timeout = 100000)
  public void testReadYourWritesSyncLeaderReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC,
                   ReplicaSelection.LEADER_ONLY);
  }

  @Test(timeout = 100000)
  public void testReadYourWritesSyncClosestReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC,
            ReplicaSelection.CLOSEST_REPLICA);
  }

  @Test(timeout = 100000)
  public void testReadYourWritesBatchLeaderReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.MANUAL_FLUSH,
                   ReplicaSelection.LEADER_ONLY);
  }

  @Test(timeout = 100000)
  public void testReadYourWritesBatchClosestReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.MANUAL_FLUSH,
            ReplicaSelection.CLOSEST_REPLICA);
  }

  private void readYourWrites(final SessionConfiguration.FlushMode flushMode,
                              final ReplicaSelection replicaSelection)
          throws Exception {
    Schema schema = createManyStringsSchema();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    final int tasksNum = 4;
    List<Callable<Void>> callables = new ArrayList<>();
    for (int t = 0; t < tasksNum; t++) {
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          AsyncKuduClient asyncKuduClient = new AsyncKuduClient
                  .AsyncKuduClientBuilder(harness.getMasterAddressesAsString())
                  .defaultAdminOperationTimeoutMs(org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP)
                  .build();
          try (KuduClient kuduClient = asyncKuduClient.syncClient()) {
            KuduSession session = kuduClient.newSession();
            session.setFlushMode(flushMode);
            KuduTable table = kuduClient.openTable(TABLE_NAME);
            for (int i = 0; i < 3; i++) {
              for (int j = 100 * i; j < 100 * (i + 1); j++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addString("key", String.format("key_%02d", j));
                row.addString("c1", "c1_" + j);
                row.addString("c2", "c2_" + j);
                row.addString("c3", "c3_" + j);
                session.apply(insert);
              }
              session.flush();

              for (int k = 0; k < 3; k++) {
                AsyncKuduScanner scanner = asyncKuduClient.newScannerBuilder(table)
                                           .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
                                           .replicaSelection(replicaSelection)
                                           .build();
                KuduScanner syncScanner = new KuduScanner(scanner);
                long preTs = asyncKuduClient.getLastPropagatedTimestamp();
                assertNotEquals(AsyncKuduClient.NO_TIMESTAMP, preTs);

                long rowCount = org.apache.kudu.test.ClientTestUtil.countRowsInScan(syncScanner);
                long expectedCount = 100L * (i + 1);
                assertTrue(expectedCount <= rowCount);

                assertNotEquals(AsyncKuduClient.NO_TIMESTAMP, scanner.getSnapshotTimestamp());
                assertTrue(preTs < scanner.getSnapshotTimestamp());
                syncScanner.close();
              }
            }
          }
          return null;
        }
      };
      callables.add(callable);
    }
    ExecutorService executor = Executors.newFixedThreadPool(tasksNum);
    List<Future<Void>> futures = executor.invokeAll(callables);
    for (Future<Void> future : futures) {
      future.get();
    }
  }
}


