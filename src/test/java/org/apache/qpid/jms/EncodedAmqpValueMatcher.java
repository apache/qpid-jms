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
package org.apache.qpid.jms;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.codec.Data;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class EncodedAmqpValueMatcher extends TypeSafeMatcher<Binary>
{
    private static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:amqp-value:*");
    private static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000077L);
    private final Object _expectedValue;
    private DescribedType _decodedDescribedType;

    /**
     * @param expectedValue the value that is expected to be in the
     * received {@link AmqpValue}
     */
    public EncodedAmqpValueMatcher(Object expectedValue)
    {
        _expectedValue = expectedValue;
    }

    @Override
    protected boolean matchesSafely(Binary receivedBinary)
    {
        Data data = Proton.data(receivedBinary.getLength());
        data.decode(receivedBinary.asByteBuffer());
        _decodedDescribedType = data.getDescribedType();
        Object descriptor = _decodedDescribedType.getDescriptor();
        if(DESCRIPTOR_CODE.equals(descriptor) || DESCRIPTOR_SYMBOL.equals(descriptor))
        {
            if(_expectedValue.equals(_decodedDescribedType.getDescribed()))
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        else
        {
            return false;
        }
    }

    @Override
    public void describeTo(Description description)
    {
        description
            .appendText("a Binary encoding of an AmqpValue that wraps ")
            .appendValue(_expectedValue);
    }

    @Override
    protected void describeMismatchSafely(Binary item, Description mismatchDescription)
    {
        mismatchDescription.appendText("\nActual encoded form: ").appendValue(item);

        if(_decodedDescribedType != null)
        {
            mismatchDescription.appendText("\nExpected descriptor: ")
                .appendValue(DESCRIPTOR_SYMBOL)
                .appendText(" / ")
                .appendValue(DESCRIPTOR_CODE);

            mismatchDescription.appendText("\nActual described type: ").appendValue(_decodedDescribedType);
        }
    }


}