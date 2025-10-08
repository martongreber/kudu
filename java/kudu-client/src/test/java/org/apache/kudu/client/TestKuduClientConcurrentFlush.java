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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.Schema;
import org.apache.kudu.test.cluster.KuduBinaryInfo;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.RandomUtils;
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig;

public class TestKuduClientConcurrentFlush {
  private static final String TABLE_NAME = "TestKuduClientConcurrentFlush";

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
    asyncClient = harness.getAsyncClient();
  }

  @MasterServerConfig(flags = {
      "--table_locations_ttl_ms=500",
  })
  @Test(timeout = 100000)
  public void testConcurrentFlush() throws Exception {
    org.junit.Assume.assumeTrue("this scenario is to run against non-sanitized binaries only",
        KuduBinaryInfo.getSanitizerType() == KuduBinaryInfo.SanitizerType.NONE);
    try {
      AsyncKuduSession.injectLatencyBufferFlushCb(true);

      CreateTableOptions opts = new CreateTableOptions()
          .addHashPartitions(ImmutableList.of("key"), 8)
          .setRangePartitionColumns(ImmutableList.of("key"));

      Schema schema = org.apache.kudu.test.ClientTestUtil.getBasicSchema();
      PartialRow lowerBoundA = schema.newPartialRow();
      PartialRow upperBoundA = schema.newPartialRow();
      upperBoundA.addInt("key", 0);
      opts.addRangePartition(lowerBoundA, upperBoundA);

      PartialRow lowerBoundB = schema.newPartialRow();
      lowerBoundB.addInt("key", 0);
      PartialRow upperBoundB = schema.newPartialRow();
      opts.addRangePartition(lowerBoundB, upperBoundB);

      KuduTable table = client.createTable(TABLE_NAME, schema, opts);

      final CountDownLatch keepRunning = new CountDownLatch(1);
      final int numSessions = 50;

      List<KuduSession> sessions = new ArrayList<>(numSessions);
      for (int i = 0; i < numSessions; ++i) {
        KuduSession session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        sessions.add(session);
      }

      List<Thread> flushers = new ArrayList<>(numSessions);
      java.util.Random random = RandomUtils.getRandom();
      {
        for (int idx = 0; idx < numSessions; ++idx) {
          final int threadIdx = idx;
          Thread flusher = new Thread(new Runnable() {
            @Override
            public void run() {
              KuduSession session = sessions.get(threadIdx);
              try {
                while (!keepRunning.await(random.nextInt(250), TimeUnit.MILLISECONDS)) {
                  session.flush();
                  assertEquals(0, session.countPendingErrors());
                }
              } catch (Exception e) {
                fail("unexpected exception: " + e);
              }
            }
          });
          flushers.add(flusher);
        }
      }

      final int numRowsPerSession = 10000;
      final CountDownLatch insertersCompleted = new CountDownLatch(numSessions);
      List<Thread> inserters = new ArrayList<>(numSessions);
      {
        for (int idx = 0; idx < numSessions; ++idx) {
          final int threadIdx = idx;
          final int keyStart = threadIdx * numRowsPerSession;
          Thread inserter = new Thread(new Runnable() {
            @Override
            public void run() {
              KuduSession session = sessions.get(threadIdx);
              try {
                for (int key = keyStart; key < keyStart + numRowsPerSession; ++key) {
                  Insert insert = org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert(table, key);
                  assertNull(session.apply(insert));
                }
                session.flush();
              } catch (Exception e) {
                fail("unexpected exception: " + e);
              }
              insertersCompleted.countDown();
            }
          });
          inserters.add(inserter);
        }
      }

      for (Thread flusher : flushers) {
        flusher.start();
      }
      for (Thread inserter : inserters) {
        inserter.start();
      }

      insertersCompleted.await();
      keepRunning.countDown();

      for (Thread inserter : inserters) {
        inserter.join();
      }
      for (Thread flusher : flushers) {
        flusher.join();
      }

      KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
          .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
          .build();
      assertEquals(numSessions * numRowsPerSession, org.apache.kudu.test.ClientTestUtil.countRowsInScan(scanner));
    } finally {
      AsyncKuduSession.injectLatencyBufferFlushCb(false);
    }
  }
}


