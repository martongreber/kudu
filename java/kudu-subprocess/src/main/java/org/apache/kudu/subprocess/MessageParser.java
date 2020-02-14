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

package org.apache.kudu.subprocess;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.WireProtocol.AppStatusPB;
import org.apache.kudu.subprocess.Subprocess.SubprocessRequestPB;
import org.apache.kudu.subprocess.Subprocess.SubprocessResponsePB;

/**
 * The {@link MessageParser} class,
 *    1. retrieves one message from the inbound queue at a time,
 *    2. processes the message and generates a response,
 *    3. and then puts the response to the outbound queue.
 */
@InterfaceAudience.Private
class MessageParser implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MessageParser.class);
  private final BlockingQueue<byte[]> inboundQueue;
  private final BlockingQueue<SubprocessResponsePB> outboundQueue;
  private final ProtocolHandler protocolHandler;

  MessageParser(BlockingQueue<byte[]> inboundQueue,
                BlockingQueue<SubprocessResponsePB> outboundQueue,
                ProtocolHandler protocolHandler) {
    Preconditions.checkNotNull(inboundQueue);
    Preconditions.checkNotNull(outboundQueue);
    this.inboundQueue = inboundQueue;
    this.outboundQueue = outboundQueue;
    this.protocolHandler = protocolHandler;
  }

  @Override
  public void run() {
    while (true) {
      byte[] data = QueueUtil.take(inboundQueue);
      SubprocessResponsePB response = getResponse(data);
      QueueUtil.put(outboundQueue, response);
    }
  }

  /**
   * Constructs a message with the given error status.
   *
   * @param errorCode the given error status
   * @param resp the message builder
   * @return a message with the given error status
   */
  static SubprocessResponsePB responseWithError(AppStatusPB.ErrorCode errorCode,
                                                SubprocessResponsePB.Builder resp) {
    Preconditions.checkNotNull(resp);
    AppStatusPB.Builder errorBuilder = AppStatusPB.newBuilder();
    errorBuilder.setCode(errorCode);
    resp.setError(errorBuilder);
    return resp.build();
  }

  /**
   * Parses the given protobuf message. If encountered InvalidProtocolBufferException,
   * which indicates the message is invalid, respond with an error message.
   *
   * @param data the protobuf message
   * @return a SubprocessResponsePB
   */
  private SubprocessResponsePB getResponse(byte[] data) {
    SubprocessResponsePB response;
    SubprocessResponsePB.Builder responseBuilder = SubprocessResponsePB.newBuilder();
    try {
      // Parses the data as a message of SubprocessRequestPB type.
      SubprocessRequestPB request = SubprocessRequestPB.parser().parseFrom(data);
      response = protocolHandler.handleRequest(request);
    } catch (InvalidProtocolBufferException e) {
      LOG.warn(String.format("%s: %s", "Unable to parse the protobuf message",
                             new String(data, StandardCharsets.UTF_8)), e);
      response = responseWithError(AppStatusPB.ErrorCode.ILLEGAL_STATE, responseBuilder);
    }
    return response;
  }
}
