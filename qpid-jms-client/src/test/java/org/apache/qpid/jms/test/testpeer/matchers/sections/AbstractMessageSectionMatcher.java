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
package org.apache.qpid.jms.test.testpeer.matchers.sections;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.codec.Data;
import org.hamcrest.Matcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMessageSectionMatcher
{
    private final Logger _logger = LoggerFactory.getLogger(getClass());

    private final UnsignedLong _numericDescriptor;
    private final Symbol _symbolicDescriptor;

    private final Map<Object, Matcher<?>> _fieldMatchers;
    private Map<Object, Object> _receivedFields;

    private final boolean _expectTrailingBytes;

    protected AbstractMessageSectionMatcher(UnsignedLong numericDescriptor,
                                            Symbol symbolicDescriptor,
                                            Map<Object, Matcher<?>> fieldMatchers,
                                            boolean expectTrailingBytes)
    {
        _numericDescriptor = numericDescriptor;
        _symbolicDescriptor = symbolicDescriptor;
        _fieldMatchers = fieldMatchers;
        _expectTrailingBytes = expectTrailingBytes;
    }

    protected Map<Object, Matcher<?>> getMatchers()
    {
        return _fieldMatchers;
    }

    protected Map<Object, Object> getReceivedFields()
    {
        return _receivedFields;
    }

    /**
     * @param receivedBinary
     *      The received Binary value that should be validated.
     *
     * @return the number of bytes consumed from the provided Binary
     *
     * @throws RuntimeException if the provided Binary does not match expectation in some way
     */
    public int verify(Binary receivedBinary) throws RuntimeException
    {
        int length = receivedBinary.getLength();
        Data data = Data.Factory.create();
        long decoded = data.decode(receivedBinary.asByteBuffer());
        if(decoded > Integer.MAX_VALUE)
        {
            throw new IllegalStateException("Decoded more bytes than Binary supports holding");
        }

        if(decoded < length && !_expectTrailingBytes)
        {
            throw new IllegalArgumentException("Expected to consume all bytes, but trailing bytes remain: Got "
                                        + length + ", consumed "+ decoded);
        }

        DescribedType decodedDescribedType = data.getDescribedType();
        verifyReceivedDescribedType(decodedDescribedType);

        //Need to cast to int, but verified earlier that it is < Integer.MAX_VALUE
        return (int) decoded;
    }

    private void verifyReceivedDescribedType(DescribedType decodedDescribedType)
    {
        Object descriptor = decodedDescribedType.getDescriptor();
        if(!(_symbolicDescriptor.equals(descriptor) || _numericDescriptor.equals(descriptor)))
        {
            throw new IllegalArgumentException("Unexpected section type descriptor. Expected "
                    + _symbolicDescriptor + " or " + _numericDescriptor + ", but got: " + descriptor);
        }

        verifyReceivedDescribedObject(decodedDescribedType.getDescribed());
    }

    /**
     * sub-classes should implement depending on the expected content of the particular section type.
     */
    protected abstract void verifyReceivedDescribedObject(Object describedObject);

    /**
     * Utility method for use by sub-classes that expect field-based sections, i.e lists or maps.
     */
    protected void verifyReceivedFields(Map<Object, Object> valueMap)
    {
        _receivedFields = valueMap;

        _logger.debug("About to check the fields of the section."
                + "\n  Received:" + valueMap
                + "\n  Expectations: " + _fieldMatchers);
        for(Map.Entry<Object, Matcher<?>> entry : _fieldMatchers.entrySet())
        {
            @SuppressWarnings("unchecked")
            Matcher<Object> matcher = (Matcher<Object>) entry.getValue();
            Object field = entry.getKey();
            assertThat("Field " + field + " value should match", valueMap.get(field), matcher);
        }
    }

    /**
     * Intended to be overridden in most cases that use the above method (but not necessarily all - hence not marked as abstract)
     */
    protected Enum<?> getField(int fieldIndex)
    {
        throw new UnsupportedOperationException("getFieldName is expected to be overridden by subclass if it is required");
    }
}