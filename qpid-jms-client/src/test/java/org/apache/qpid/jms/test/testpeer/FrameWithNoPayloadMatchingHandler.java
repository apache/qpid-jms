/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.qpid.jms.test.testpeer;

import java.util.Map;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.hamcrest.Matcher;

public class FrameWithNoPayloadMatchingHandler extends AbstractFrameFieldAndPayloadMatchingHandler
{

    protected FrameWithNoPayloadMatchingHandler(FrameType frameType,
                                                int channel,
                                                UnsignedLong numericDescriptor,
                                                Symbol symbolicDescriptor,
                                                Map<Enum<?>, Matcher<?>> matchers,
                                                Runnable onSuccess)
    {
        super(frameType, channel, numericDescriptor, symbolicDescriptor, matchers, onSuccess);
    }

    @Override
    protected void verifyPayload(Binary payload)
    {
        if(payload != null && payload.getLength() > 0)
        {
            throw new IllegalArgumentException("Expected no payload but received payload of length: " + payload.getLength());
        }
    }
}