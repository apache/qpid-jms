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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.hamcrest.Matcher;

public abstract class AbstractFrameFieldAndPayloadMatchingHandler extends FrameMatchingHandler
{
    private final Logger _logger = Logger.getLogger(getClass().getName());

    private final Map<Enum<?>, Matcher<?>> _fieldMatchers;
    private Map<Enum<?>, Object> _receivedFields;

    /**
     * @param fieldMatchers a map of field matchers, keyed by enums representing the fields
     * (the enums just need to have an ordinal number matching the AMQP spec field order,
     * and preferably a sensible name)
     */
    protected AbstractFrameFieldAndPayloadMatchingHandler(FrameType frameType,
                                                int channel,
                                                UnsignedLong numericDescriptor,
                                                Symbol symbolicDescriptor,
                                                Map<Enum<?>, Matcher<?>> fieldMatchers,
                                                Runnable onSuccess)
    {
        super(frameType, channel, numericDescriptor, symbolicDescriptor, onSuccess);
        _fieldMatchers = fieldMatchers;
    }

    protected Map<Enum<?>, Matcher<?>> getMatchers()
    {
        return _fieldMatchers;
    }

    /**
     * Returns the received values, keyed by enums representing the fields
     * (the enums have an ordinal number matching the AMQP spec field order,
     * and a sensible name)
     */
    @Override
    protected Map<Enum<?>, Object> getReceivedFields()
    {
        return _receivedFields;
    }

    @Override
    protected void verifyFrame(List<Object> described, Binary payload)
    {
        verifyFields(described);
        verifyPayload(payload);
    }

    protected void verifyFields(List<Object> described)
    {
        int fieldNumber = 0;
        HashMap<Enum<?>, Object> valueMap = new HashMap<>();
        for(Object value : described)
        {
            valueMap.put(getField(fieldNumber++), value);
        }

        _receivedFields = valueMap;

        _logger.fine("About to check the fields of the described type."
                + "\n  Received:" + valueMap
                + "\n  Expectations: " + _fieldMatchers);
        for(Map.Entry<Enum<?>, Matcher<?>> entry : _fieldMatchers.entrySet())
        {
            @SuppressWarnings("unchecked")
            Matcher<Object> matcher = (Matcher<Object>) entry.getValue();
            Enum<?> field = entry.getKey();
            assertThat("Field " + field + " value should match", valueMap.get(field), matcher);
        }
    }

    /**
     * Intended to be overridden in most cases (but not necessarily all - hence not marked as abstract)
     */
    protected Enum<?> getField(int fieldIndex)
    {
        throw new UnsupportedOperationException("getFieldName is expected to be overridden by subclass if it is required");
    }

    protected abstract void verifyPayload(Binary payload);
}