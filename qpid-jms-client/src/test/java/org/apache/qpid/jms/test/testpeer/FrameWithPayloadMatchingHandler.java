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

import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.hamcrest.Matcher;

public class FrameWithPayloadMatchingHandler extends AbstractFrameFieldAndPayloadMatchingHandler
{
    private Matcher<Binary> _payloadMatcher;
    private Binary _receivedPayload;

    protected FrameWithPayloadMatchingHandler(FrameType frameType,
                                                int channel,
                                                UnsignedLong numericDescriptor,
                                                Symbol symbolicDescriptor)
    {
        super(frameType, channel, 0, numericDescriptor, symbolicDescriptor);
    }

    protected FrameWithPayloadMatchingHandler(FrameType frameType,
                                                int channel,
                                                int frameSize,
                                                UnsignedLong numericDescriptor,
                                                Symbol symbolicDescriptor)
    {
        super(frameType, channel, frameSize, numericDescriptor, symbolicDescriptor);
    }

    public void setPayloadMatcher(Matcher<Binary> payloadMatcher)
    {
        _payloadMatcher = payloadMatcher;
    }

    @Override
    protected final void verifyPayload(Binary payload) throws AssertionError
    {
        _logger.debug("About to check the payload" + "\n  Received: {}", payload);
        _receivedPayload = payload;
        if(_payloadMatcher != null)
        {
            assertThat("Payload should match", payload, _payloadMatcher);
        }
    }

    public Binary getReceivedPayload()
    {
        return _receivedPayload;
    }
}