/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.provider.amqp;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.security.SaslFrameBody;
import org.apache.qpid.proton.engine.impl.ProtocolTracer;
import org.apache.qpid.proton.framing.TransportFrame;
import org.slf4j.Logger;

/**
 * Proton-J ProtocolTracer implementation that writes AMQP frame data to the
 * given logger target.
 */
public class AmqpProtocolTracer implements ProtocolTracer {

    public static final int DEFAULT_PAYLOAD_STRING_LIMIT = 1024;

    private final Logger logger;
    private final int transportIdentifier;
    private final int payloadStringLimit;

    public AmqpProtocolTracer(Logger logger, int transportIdentifier) {
        this(logger, transportIdentifier, DEFAULT_PAYLOAD_STRING_LIMIT);
    }

    public AmqpProtocolTracer(Logger logger, int transportIdentifier, int payloadStringLimit) {
        this.logger = logger;
        this.payloadStringLimit = payloadStringLimit;
        this.transportIdentifier = transportIdentifier;
    }

    @Override
    public void receivedFrame(TransportFrame transportFrame) {
        logger.trace("[{}:{}] RECV: {}{}", transportIdentifier, transportFrame.getChannel(), transportFrame.getBody(), formatPayload(transportFrame));
    }

    @Override
    public void sentFrame(TransportFrame transportFrame) {
        logger.trace("[{}:{}] SENT: {}{}", transportIdentifier, transportFrame.getChannel(), transportFrame.getBody(), formatPayload(transportFrame));
    }

    @Override
    public void receivedSaslBody(SaslFrameBody saslFrameBody) {
        logger.trace("[{}:0] RECV: {}", transportIdentifier, saslFrameBody);
    };

    @Override
    public void sentSaslBody(SaslFrameBody saslFrameBody) {
        logger.trace("[{}:0] SENT: {}", transportIdentifier, saslFrameBody);
    };

    @Override
    public void receivedHeader(String header) {
        logger.trace("[{}:0] RECV: {}", transportIdentifier, header);
    };

    @Override
    public void sentHeader(String header) {
        logger.trace("[{}:0] SENT: {}", transportIdentifier, header);
    };

    private String formatPayload(TransportFrame frame) {
        Binary payload = frame.getPayload();

        if (payload == null || payload.getLength() == 0 || payloadStringLimit <= 0) {
            return "";
        }

        final byte[] binData = payload.getArray();
        final int binLength = payload.getLength();
        final int offset = payload.getArrayOffset();

        StringBuilder builder = new StringBuilder();

        // Prefix the payload with total bytes which gives insight regardless of truncation.
        builder.append(" (").append(payload.getLength()).append(") ").append("\"");

        int size = 0;
        boolean truncated = false;
        for (int i = 0; i < binLength; i++) {
            byte c = binData[offset + i];

            if (c > 31 && c < 127 && c != '\\') {
                if (size + 1 <= payloadStringLimit) {
                    size += 1;
                    builder.append((char) c);
                } else {
                    truncated = true;
                    break;
                }
            } else {
                if (size + 4 <= payloadStringLimit) {
                    size += 4;
                    builder.append(String.format("\\x%02x", c));
                } else {
                    truncated = true;
                    break;
                }
            }
        }

        builder.append("\"");

        if (truncated) {
            builder.append("...(truncated)");
        }

        return builder.toString();
    }
}
