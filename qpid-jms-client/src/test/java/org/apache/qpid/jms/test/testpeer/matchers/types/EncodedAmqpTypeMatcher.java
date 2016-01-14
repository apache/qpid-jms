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

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.codec.Data;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public abstract class EncodedAmqpTypeMatcher extends TypeSafeMatcher<Binary>
{
    private final Symbol _descriptorSymbol;
    private final UnsignedLong _descriptorCode;
    private final Object _expectedValue;
    private boolean _permitTrailingBytes;
    private DescribedType _decodedDescribedType;
    private boolean _unexpectedTrailingBytes;

    public EncodedAmqpTypeMatcher(Symbol symbol, UnsignedLong code, Object expectedValue)
    {
        this(symbol, code, expectedValue, false);
    }

    public EncodedAmqpTypeMatcher(Symbol symbol, UnsignedLong code, Object expectedValue, boolean permitTrailingBytes)
    {
        _descriptorSymbol = symbol;
        _descriptorCode = code;
        _expectedValue = expectedValue;
        _permitTrailingBytes = permitTrailingBytes;
    }

    protected Object getExpectedValue()
    {
        return _expectedValue;
    }

    @Override
    protected boolean matchesSafely(Binary receivedBinary)
    {
        int length = receivedBinary.getLength();
        Data data = Data.Factory.create();
        long decoded = data.decode(receivedBinary.asByteBuffer());
        _decodedDescribedType = data.getDescribedType();
        Object descriptor = _decodedDescribedType.getDescriptor();

        if(!(_descriptorCode.equals(descriptor) || _descriptorSymbol.equals(descriptor)))
        {
            return false;
        }

        if(_expectedValue == null && _decodedDescribedType.getDescribed() != null)
        {
            return false;
        }
        else if(_expectedValue != null && !_expectedValue.equals(_decodedDescribedType.getDescribed()))
        {
            return false;
        }

        if(decoded < length && !_permitTrailingBytes)
        {
            _unexpectedTrailingBytes = true;
            return false;
        }

        return true;
    }

    @Override
    protected void describeMismatchSafely(Binary item, Description mismatchDescription)
    {
        mismatchDescription.appendText("\nActual encoded form: ").appendValue(item);

        if(_decodedDescribedType != null)
        {
            mismatchDescription.appendText("\nExpected descriptor: ")
                .appendValue(_descriptorSymbol)
                .appendText(" / ")
                .appendValue(_descriptorCode);

            mismatchDescription.appendText("\nActual described type: ").appendValue(_decodedDescribedType);
        }

        if(_unexpectedTrailingBytes)
        {
            mismatchDescription.appendText("\nUnexpected trailing bytes in provided bytes after decoding!");
        }
    }

    /**
     * Provide a description of this matcher.
     */
    public abstract void describeTo(Description description);
}