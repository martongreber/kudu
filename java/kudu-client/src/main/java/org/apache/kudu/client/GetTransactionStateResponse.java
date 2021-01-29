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

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.transactions.Transactions;

@InterfaceAudience.Private
public class GetTransactionStateResponse extends KuduRpcResponse {
  private final Transactions.TxnStatePB txnState;

  /**
   * @param elapsedMillis time in milliseconds since RPC creation to now
   * @param serverUUID UUID of the server that sent the response
   * @param txnState the state of the transaction
   */
  GetTransactionStateResponse(
      long elapsedMillis, String serverUUID, Transactions.TxnStatePB txnState) {
    super(elapsedMillis, serverUUID);
    this.txnState = txnState;
  }

  public Transactions.TxnStatePB txnState() {
    return txnState;
  }

  public boolean isCommitted() {
    return txnState == Transactions.TxnStatePB.COMMITTED;
  }

  public boolean isAborted() {
    return txnState == Transactions.TxnStatePB.ABORTED;
  }
}
