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
package org.apache.qpid.jms.test.testpeer.matchers.types;

import java.util.List;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.hamcrest.Description;

public class EncodedAmqpSequenceMatcher extends EncodedAmqpTypeMatcher
{
    private static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:amqp-sequence:list");
    private static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000076L);

    /**
     * @param expectedValue the value that is expected to be IN the
     * received {@link AmqpSequence}
     */
    public EncodedAmqpSequenceMatcher(List<Object> expectedValue)
    {
        this(expectedValue,false);
    }

    /**
     * @param expectedValue the value that is expected to be IN the
     * received {@link AmqpSequence}
     * @param permitTrailingBytes if it is permitted for bytes to be left in the Binary after consuming the {@link AmqpSequence}
     */
    public EncodedAmqpSequenceMatcher(Object expectedValue, boolean permitTrailingBytes)
    {
        super(DESCRIPTOR_SYMBOL, DESCRIPTOR_CODE, expectedValue, permitTrailingBytes);
    }

    @Override
    public void describeTo(Description description)
    {
        description
            .appendText("a Binary encoding of an AmqpSequence that wraps: ")
            .appendValue(getExpectedValue());
    }
}