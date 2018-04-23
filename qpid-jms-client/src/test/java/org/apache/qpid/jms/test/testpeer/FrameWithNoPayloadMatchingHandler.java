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

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;

public class FrameWithNoPayloadMatchingHandler extends AbstractFrameFieldAndPayloadMatchingHandler
{

    protected FrameWithNoPayloadMatchingHandler(FrameType frameType,
                                                int channel,
                                                UnsignedLong numericDescriptor,
                                                Symbol symbolicDescriptor)
    {
        super(frameType, channel, 0, numericDescriptor, symbolicDescriptor);
    }

    @Override
    protected final void verifyPayload(Binary payload)
    {
        _logger.debug("About to check that there is no payload");
        if(payload != null && payload.getLength() > 0)
        {
            throw new AssertionError("Expected no payload but received payload of length: " + payload.getLength());
        }
    }
}