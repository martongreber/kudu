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

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.CapturingLogAppender;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.LocationConfig;
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig;

public class TestKuduClientResilience {
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
  public void testGetAuthnToken() throws Exception {
    byte[] token = asyncClient.exportAuthenticationCredentials().join();
    assertNotNull(token);
  }

  @Test(timeout = 100000)
  public void testCloseShortlyAfterOpen() throws Exception {
    CapturingLogAppender cla = new CapturingLogAppender();
    try (Closeable c = cla.attach()) {
      try (KuduClient localClient =
               new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build()) {
        localClient.exportAuthenticationCredentials();
      }
      Thread.sleep(500);
    }
    assertFalse(cla.getAppendedText(), cla.getAppendedText().contains("Exception"));
  }

  @Test(timeout = 100000)
  public void testNoLogSpewOnConnectionRefused() throws Exception {
    CapturingLogAppender cla = new CapturingLogAppender();
    try (Closeable c = cla.attach()) {
      harness.killAllMasterServers();
      try (KuduClient localClient =
               new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build()) {
        localClient.exportAuthenticationCredentials();
        fail("Should have failed to connect.");
      } catch (NonRecoverableException e) {
        assertTrue("Bad exception string: " + e.getMessage(),
            e.getMessage().matches(".*Master config .+ has no leader. " +
                "Exceptions received:.*Connection refused.*Connection refused" +
                ".*Connection refused.*"));
      }
    } finally {
      harness.startAllMasterServers();
    }
    String logText = cla.getAppendedText();
    assertFalse("Should not claim to have lost a connection in the log",
               logText.contains("lost connection to peer"));
    assertFalse("Should not have netty spew in log",
                logText.contains("socket.nio.AbstractNioSelector"));
  }

  @Test(timeout = 100000)
  public void testCustomNioExecutor() throws Exception {
    long startTime = System.nanoTime();
    try (KuduClient localClient =
             new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString())
                 .nioExecutors(java.util.concurrent.Executors.newFixedThreadPool(1),
                     java.util.concurrent.Executors.newFixedThreadPool(2))
                 .bossCount(1)
                 .workerCount(2)
                 .build()) {
      long buildTime = (System.nanoTime() - startTime) / 1000000000L;
      assertTrue("Building KuduClient is slow, maybe netty get stuck", buildTime < 3);
      localClient.createTable("TestKuduClientResilience", org.apache.kudu.test.ClientTestUtil.getBasicSchema(), org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions());
      Thread[] threads = new Thread[4];
      for (int t = 0; t < 4; t++) {
        final int id = t;
        threads[t] = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              KuduTable table = localClient.openTable("TestKuduClientResilience");
              KuduSession session = localClient.newSession();
              session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
              for (int i = 0; i < 100; i++) {
                Insert insert = org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert(table, id * 100 + i);
                session.apply(insert);
              }
              session.close();
            } catch (Exception e) {
              fail("insert thread should not throw exception: " + e);
            }
          }
        });
        threads[t].start();
      }
      for (int t = 0; t < 4; t++) {
        threads[t].join();
      }
    }
  }

  private void runTestCallDuringLeaderElection(String clientMethodName) throws Exception {
    java.lang.reflect.Method methodToInvoke = KuduClient.class.getMethod(clientMethodName);

    for (int i = 0; i < 5; i++) {
      try (KuduClient cl = new KuduClient.KuduClientBuilder(
          harness.getMasterAddressesAsString()).build()) {
        harness.restartLeaderMaster();

        methodToInvoke.invoke(cl);
      }
    }

    harness.killAllMasterServers();
    try (KuduClient cl = new KuduClient.KuduClientBuilder(
        harness.getMasterAddressesAsString())
         .defaultAdminOperationTimeoutMs(5000)
         .build()) {
      try {
        methodToInvoke.invoke(cl);
        fail();
      } catch (InvocationTargetException ex) {
        assertTrue(ex.getTargetException() instanceof KuduException);
        KuduException realEx = (KuduException) ex.getTargetException();
        assertTrue(realEx.getStatus().isTimedOut());
      }
    }
  }

  @Test(timeout = 100000)
  public void testExportAuthenticationCredentialsDuringLeaderElection() throws Exception {
    runTestCallDuringLeaderElection("exportAuthenticationCredentials");
  }

  @Test(timeout = 100000)
  public void testGetHiveMetastoreConfigDuringLeaderElection() throws Exception {
    runTestCallDuringLeaderElection("getHiveMetastoreConfig");
  }

  @Test(timeout = 100000)
  public void testClientLocationNoLocation() throws Exception {
    client.listTabletServers();
    assertEquals("", client.getLocationString());
  }

  @Test(timeout = 100000)
  @LocationConfig(locations = {
      "/L0:6",
  })
  @MasterServerConfig(flags = {
      "--master_client_location_assignment_enabled=true",
  })
  public void testClientLocation() throws Exception {
    client.listTabletServers();
    assertEquals("/L0", client.getLocationString());
  }

  @Test(timeout = 100000)
  public void testClusterId() throws Exception {
    assertTrue(client.getClusterId().isEmpty());
    client.listTabletServers();
    assertFalse(client.getClusterId().isEmpty());
  }

  @Test(timeout = 50000)
  public void testImportInvalidCert() throws Exception {
    byte[] caCert = new byte[0];
    java.security.cert.CertificateException e = org.junit.Assert.assertThrows(java.security.cert.CertificateException.class, () -> {
      client.trustedCertificates(java.util.Arrays.asList(com.google.protobuf.ByteString.copyFrom(caCert)));
    });
    assertTrue(e.getMessage().contains("Could not parse certificate"));
  }
}


